package client_conn

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"github.com/fengleng/go-mysql-client/mysql"
	"github.com/fengleng/log"
	"github.com/pingcap/errors"
)

var (
	ErrAccessDenied           = errors.New("access denied")
	ErrAccessDeniedNoPassword = fmt.Errorf("%w without password", ErrAccessDenied)
)

func (c *ClientConn) Handshake() error {
	if err := c.writeInitialHandshake(); err != nil {
		log.Error("server Handshake %v %d msg send initial handshake error", err, c.connectionId)
		return errors.Trace(err)
	}

	if err := c.readHandshakeResponse(); err != nil {
		log.Error("server readHandshakeResponse %v %d msg read Handshake Response error", err, c.connectionId)
		return errors.Trace(err)
	}
	if err := c.writeOK(nil); err != nil {
		log.Error("server readHandshakeResponse %v write ok fail %d ", err, c.connectionId)
		return errors.Trace(err)
	}
	c.pkg.Sequence = 0
	return nil
}

func (c *ClientConn) writeInitialHandshake() error {
	data := make([]byte, 4, 128)

	//min version 10
	data = append(data, 10)

	//server version[00]
	data = append(data, mysql.ServerVersion...)
	data = append(data, 0)

	//connection id
	data = append(data, byte(c.connectionId), byte(c.connectionId>>8), byte(c.connectionId>>16), byte(c.connectionId>>24))

	//auth-plugin-data-part-1
	data = append(data, c.salt[0:8]...)

	//filter [00]
	data = append(data, 0)

	//capability flag lower 2 bytes, using default capability here
	data = append(data, byte(DEFAULT_CAPABILITY), byte(DEFAULT_CAPABILITY>>8))

	//charset, utf-8 default
	data = append(data, uint8(mysql.DEFAULT_COLLATION_ID))

	//status
	data = append(data, byte(c.status), byte(c.status>>8))

	//below 13 byte may not be used
	//capability flag upper 2 bytes, using default capability here
	data = append(data, byte(DEFAULT_CAPABILITY>>16), byte(DEFAULT_CAPABILITY>>24))

	//filter [0x15], for wireshark dump, value is 0x15
	data = append(data, 0x15)

	//reserved 10 [00]
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

	//auth-plugin-data-part-2
	data = append(data, c.salt[8:]...)

	//filter [00]
	data = append(data, 0)

	return c.writePacket(data)
}

func (c *ClientConn) readHandshakeResponse() error {
	data, err := c.readPacket()

	if err != nil {
		return err
	}

	pos := 0

	//capability
	c.capability = binary.LittleEndian.Uint32(data[:4])
	pos += 4

	//skip max packet size
	pos += 4

	//charset, skip, if you want to use another charset, use set names
	//c.collation = CollationId(data[pos])
	pos++

	//skip reserved 23[00]
	pos += 23

	//user name
	c.user = string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])

	pos += len(c.user) + 1

	//auth length and auth
	authLen := int(data[pos])
	pos++
	auth := data[pos : pos+authLen]

	//check user
	//if _, ok := c.proxy.Users[c.user]; !ok {
	//	log.Error("ClientConn ClientConnId %d client_user acce")
	//	golog.Error("ClientConn", "readHandshakeResponse", "error", 0,
	//		"auth", auth,
	//		"client_user", c.user,
	//		"config_set_user", c.user,
	//		"password", c.proxy.users[c.user])
	//	return mysql.NewDefaultError(mysql.ER_ACCESS_DENIED_ERROR, c.user, c.c.RemoteAddr().String(), "Yes")
	//}

	//check password
	//checkAuth := mysql.CalcPassword(c.salt, []byte(c.proxy.Users[c.user]))
	//if !bytes.Equal(auth, checkAuth) {
	//	log.Error("client user:%s RemoteAddr %s", "ER_ACCESS_DENIED_ERROR", c.c.RemoteAddr().String())
	//	return mysql.NewDefaultError(mysql.ER_ACCESS_DENIED_ERROR, c.user, c.c.RemoteAddr().String(), "Yes")
	//}

	pos += authLen

	var db string
	if c.capability&mysql.CLIENT_CONNECT_WITH_DB > 0 {
		if len(data[pos:]) > 0 {
			db = string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])
			pos += len(c.db) + 1
		}
	}
	c.db = db

	//if c.capability&mysql.CLIENT_PLUGIN_AUTH != 0 {
	//	c.authPluginName = string(data[pos : pos+bytes.IndexByte(data[pos:], 0x00)])
	//	pos += len(c.authPluginName)
	//} else {
	//	// The method used is Native Authentication if both CLIENT_PROTOCOL_41 and CLIENT_SECURE_CONNECTION are set,
	//	// but CLIENT_PLUGIN_AUTH is not set, so we fallback to 'mysql_native_password'
	//	c.authPluginName = mysql.AUTH_NATIVE_PASSWORD
	//}
	c.authPluginName = mysql.AUTH_NATIVE_PASSWORD
	cont, err := c.handleAuthMatch()
	if err != nil {
		return err
	}
	if !cont {
		return nil
	}

	// ignore connect attrs for now, the proxy does not support passing attrs to actual MySQL server

	// try to authenticate the client
	return c.compareAuthData(c.authPluginName, auth)
}

func (c *ClientConn) handleAuthMatch() (bool, error) {
	// if the client responds the handshake with a different auth method, the server will send the AuthSwitchRequest packet
	// to the client to ask the client to switch.

	if c.authPluginName != c.srv.Cfg.DefaultAuthMethod {
		if err := c.writeAuthSwitchRequest(c.srv.Cfg.DefaultAuthMethod); err != nil {
			return false, err
		}
		c.authPluginName = c.srv.Cfg.DefaultAuthMethod
		// handle AuthSwitchResponse
		return false, c.handleAuthSwitchResponse()
	}
	return true, nil
}

func (c *ClientConn) compareAuthData(authPluginName string, clientAuthData []byte) error {
	switch authPluginName {
	case mysql.AUTH_NATIVE_PASSWORD:
		//if err := c.acquirePassword(); err != nil {
		//	return err
		//}
		return c.compareNativePasswordAuthData(clientAuthData, c.srv.Users[c.user])

	case mysql.AUTH_CACHING_SHA2_PASSWORD:
		if err := c.compareCacheSha2PasswordAuthData(clientAuthData); err != nil {
			return err
		}
		if c.cachingSha2FullAuth {
			return c.handleAuthSwitchResponse()
		}
		return nil

	case mysql.AUTH_SHA256_PASSWORD:
		//if err := c.acquirePassword(); err != nil {
		//	return err
		//}
		cont, err := c.handlePublicKeyRetrieval(clientAuthData)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
		return c.compareSha256PasswordAuthData(clientAuthData, c.srv.Users[c.user])

	default:
		return errors.Errorf("unknown authentication plugin name '%s'", authPluginName)
	}
}

func (c *ClientConn) compareSha256PasswordAuthData(clientAuthData []byte, password string) error {
	// Empty passwords are not hashed, but sent as empty string
	if len(clientAuthData) == 0 {
		if password == "" {
			return nil
		}
		return ErrAccessDenied
	}
	if tlsConn, ok := c.c.(*tls.Conn); ok {
		if !tlsConn.ConnectionState().HandshakeComplete {
			return errors.New("incomplete TSL handshake")
		}
		// connection is SSL/TLS, client should send plain password
		// deal with the trailing \NUL added for plain text password received
		if l := len(clientAuthData); l != 0 && clientAuthData[l-1] == 0x00 {
			clientAuthData = clientAuthData[:l-1]
		}
		if bytes.Equal(clientAuthData, []byte(password)) {
			return nil
		}
		return errAccessDenied(password)
	} else {
		// client should send encrypted password
		// decrypt
		dbytes, err := rsa.DecryptOAEP(sha1.New(), rand.Reader, (c.srv.TlsCfg.Certificates[0].PrivateKey).(*rsa.PrivateKey), clientAuthData, nil)
		if err != nil {
			return err
		}
		plain := make([]byte, len(password)+1)
		copy(plain, password)
		for i := range plain {
			j := i % len(c.salt)
			plain[i] ^= c.salt[j]
		}
		if bytes.Equal(plain, dbytes) {
			return nil
		}
		return errAccessDenied(password)
	}
}

func (c *ClientConn) compareNativePasswordAuthData(clientAuthData []byte, password string) error {
	if bytes.Equal(mysql.CalcPassword(c.salt, []byte(password)), clientAuthData) {
		return nil
	}
	return errAccessDenied(password)
}

func errAccessDenied(password string) error {
	if password == "" {
		return ErrAccessDeniedNoPassword
	}

	return ErrAccessDenied
}

// Public Key Retrieval
// See: https://dev.mysql.com/doc/internals/en/public-key-retrieval.html
func (c *ClientConn) handlePublicKeyRetrieval(authData []byte) (bool, error) {
	// if the client use 'sha256_password' auth method, and request for a public key
	// we send back a keyfile with Protocol::AuthMoreData
	if c.authPluginName == mysql.AUTH_SHA256_PASSWORD && len(authData) == 1 && authData[0] == mysql.MORE_DATE_HEADER {
		if c.capability&mysql.CLIENT_SSL == 0 {
			return false, errors.New("server does not support SSL: CLIENT_SSL not enabled")
		}
		if err := c.writeAuthMoreDataPubkey(); err != nil {
			return false, err
		}

		return false, c.handleAuthSwitchResponse()
	}
	return true, nil
}

func (c *ClientConn) writeAuthMoreDataPubkey() error {
	data := make([]byte, 4)
	data = append(data, mysql.MORE_DATE_HEADER)
	data = append(data, c.srv.PubKey...)
	return c.writePacket(data)
}

// see: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_switch_request.html
func (c *ClientConn) writeAuthSwitchRequest(newAuthPluginName string) error {
	data := make([]byte, 4)
	data = append(data, mysql.EOF_HEADER)
	data = append(data, []byte(newAuthPluginName)...)
	data = append(data, 0x00)
	rnd, err := mysql.RandomBuf(20)
	if err != nil {
		return err
	}
	// new auth data
	c.salt = rnd
	data = append(data, c.salt...)
	// the online doc states it's a string.EOF, however, the actual MySQL server add a \NUL to the end, without it, the
	// official MySQL client will fail.
	data = append(data, 0x00)
	return c.writePacket(data)
}

func (c *ClientConn) handleAuthSwitchResponse() error {
	authData, err := c.readAuthSwitchRequestResponse()
	if err != nil {
		return err
	}

	switch c.authPluginName {
	case mysql.AUTH_NATIVE_PASSWORD:
		//if err := c.acquirePassword(); err != nil {
		//	return err
		//}
		if !bytes.Equal(mysql.CalcPassword(c.salt, []byte(c.srv.Users[c.user])), authData) {
			return mysql.NewError(mysql.ER_ACCESS_DENIED_ERROR, fmt.Sprintf("user:%s", c.user))
		}
		return nil

	case mysql.AUTH_CACHING_SHA2_PASSWORD:
		if !c.cachingSha2FullAuth {
			// Switched auth method but no MoreData packet send yet
			if err := c.compareCacheSha2PasswordAuthData(authData); err != nil {
				return err
			} else {
				if c.cachingSha2FullAuth {
					return c.handleAuthSwitchResponse()
				}
				return nil
			}
		}
		// AuthMoreData packet already sent, do full auth
		if err := c.handleCachingSha2PasswordFullAuth(authData); err != nil {
			return err
		}
		c.writeCachingSha2Cache()
		return nil

	case mysql.AUTH_SHA256_PASSWORD:
		cont, err := c.handlePublicKeyRetrieval(authData)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
		//if err := c.acquirePassword(); err != nil {
		//	return err
		//}
		return c.compareSha256PasswordAuthData(authData, c.srv.Users[c.user])

	default:
		return errors.Errorf("unknown authentication plugin name '%s'", c.authPluginName)
	}
}

// see: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_switch_response.html
func (c *ClientConn) readAuthSwitchRequestResponse() ([]byte, error) {
	data, err := c.readPacket()
	if err != nil {
		return nil, err
	}
	if len(data) == 1 && data[0] == 0x00 {
		// \NUL
		return make([]byte, 0), nil
	}
	return data, nil
}

func scrambleValidation(cached, nonce, scramble []byte) bool {
	// SHA256(SHA256(SHA256(STORED_PASSWORD)), NONCE)
	crypt := sha256.New()
	crypt.Write(cached)
	crypt.Write(nonce)
	message2 := crypt.Sum(nil)
	// SHA256(PASSWORD)
	if len(message2) != len(scramble) {
		return false
	}
	for i := range message2 {
		message2[i] ^= scramble[i]
	}
	// SHA256(SHA256(PASSWORD)
	crypt.Reset()
	crypt.Write(message2)
	m := crypt.Sum(nil)
	return bytes.Equal(m, cached)
}

func (c *ClientConn) compareCacheSha2PasswordAuthData(clientAuthData []byte) error {
	// Empty passwords are not hashed, but sent as empty string
	if len(clientAuthData) == 0 {
		//if err := c.acquirePassword(); err != nil {
		//	return err
		//}
		if c.srv.Users[c.user] == "" {
			return nil
		}
		return ErrAccessDenied
	}
	// the caching of 'caching_sha2_password' in MySQL, see: https://dev.mysql.com/worklog/task/?id=9591
	//if _, ok := c.credentialProvider.(*InMemoryProvider); ok {
	//	// since we have already kept the password in memory and calculate the scramble is not that high of cost, we eliminate
	//	// the caching part. So our server will never ask the client to do a full authentication via RSA key exchange and it appears
	//	// like the auth will always hit the cache.
	//	if err := c.acquirePassword(); err != nil {
	//		return err
	//	}
	//	if bytes.Equal(CalcCachingSha2Password(c.salt, c.password), clientAuthData) {
	//		// 'fast' auth: write "More data" packet (first byte == 0x01) with the second byte = 0x03
	//		return c.writeAuthMoreDataFastAuth()
	//	}
	//
	//	return errAccessDenied(c.password)
	//}
	// other type of credential provider, we use the cache
	cached, ok := c.srv.CacheShaPassword.Load(fmt.Sprintf("%s@%s", c.user, c.c.LocalAddr()))
	if ok {
		// Scramble validation
		if scrambleValidation(cached.([]byte), c.salt, clientAuthData) {
			// 'fast' auth: write "More data" packet (first byte == 0x01) with the second byte = 0x03
			return c.writeAuthMoreDataFastAuth()
		}

		return errAccessDenied(c.srv.Users[c.user])
	}
	// cache miss, do full auth
	if err := c.writeAuthMoreDataFullAuth(); err != nil {
		return err
	}
	c.cachingSha2FullAuth = true
	return nil
}

func (c *ClientConn) writeAuthMoreDataFullAuth() error {
	data := make([]byte, 4)
	data = append(data, mysql.MORE_DATE_HEADER)
	data = append(data, mysql.CACHE_SHA2_FULL_AUTH)
	return c.writePacket(data)
}

func (c *ClientConn) writeAuthMoreDataFastAuth() error {
	data := make([]byte, 4)
	data = append(data, mysql.MORE_DATE_HEADER)
	data = append(data, mysql.CACHE_SHA2_FAST_AUTH)
	return c.writePacket(data)
}

func (c *ClientConn) handleCachingSha2PasswordFullAuth(authData []byte) error {
	//if err := c.acquirePassword(); err != nil {
	//	return err
	//}
	if tlsConn, ok := c.c.(*tls.Conn); ok {
		if !tlsConn.ConnectionState().HandshakeComplete {
			return errors.New("incomplete TSL handshake")
		}
		// connection is SSL/TLS, client should send plain password
		// deal with the trailing \NUL added for plain text password received
		if l := len(authData); l != 0 && authData[l-1] == 0x00 {
			authData = authData[:l-1]
		}
		if bytes.Equal(authData, []byte(c.srv.Users[c.user])) {
			return nil
		}
		return errAccessDenied(c.srv.Users[c.user])
	} else {
		// client either request for the public key or send the encrypted password
		if len(authData) == 1 && authData[0] == 0x02 {
			// send the public key
			if err := c.writeAuthMoreDataPubkey(); err != nil {
				return err
			}
			// read the encrypted password
			var err error
			if authData, err = c.readAuthSwitchRequestResponse(); err != nil {
				return err
			}
		}
		// the encrypted password
		// decrypt
		dbytes, err := rsa.DecryptOAEP(sha1.New(), rand.Reader, (c.srv.TlsCfg.Certificates[0].PrivateKey).(*rsa.PrivateKey), authData, nil)
		if err != nil {
			return err
		}
		plain := make([]byte, len(c.srv.Users[c.user])+1)
		copy(plain, c.srv.Users[c.user])
		for i := range plain {
			j := i % len(c.salt)
			plain[i] ^= c.salt[j]
		}
		if bytes.Equal(plain, dbytes) {
			return nil
		}
		return errAccessDenied(c.srv.Users[c.user])
	}
}

func (c *ClientConn) writeCachingSha2Cache() {
	// write cache
	if c.srv.Users[c.user] == "" {
		return
	}
	// SHA256(PASSWORD)
	crypt := sha256.New()
	crypt.Write([]byte(c.srv.Users[c.user]))
	m1 := crypt.Sum(nil)
	// SHA256(SHA256(PASSWORD))
	crypt.Reset()
	crypt.Write(m1)
	m2 := crypt.Sum(nil)
	// caching_sha2_password will maintain an in-memory hash of `user`@`host` => SHA256(SHA256(PASSWORD))
	c.srv.CacheShaPassword.Store(fmt.Sprintf("%s@%s", c.user, c.c.LocalAddr()), m2)
}
