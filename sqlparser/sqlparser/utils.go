package sqlparser

// StringIn is a convenience function that returns
// true if str matches any of the values.
func StringIn(str string, values ...string) bool {
	for _, val := range values {
		if str == val {
			return true
		}
	}
	return false
}
