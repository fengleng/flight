package plan

/*2,5 ==> [2,3,4]*/
func makeList(start, end int) []int {
	list := make([]int, end-start)
	for i := start; i < end; i++ {
		list[i-start] = i
	}
	return list
}

//indexes is sequential
//if start is 2016, end is 2017. indexs is [2015,2016,2017,2018]
//the result is [2016,2017]
func makeBetweenList(start, end int, indexes []int) []int {
	var startIndex, endIndex int
	var SetStart bool
	if end < start {
		start, end = end, start
	}

	for k, v := range indexes {
		if v == start {
			startIndex = k
			SetStart = true
		}
		if v == end {
			endIndex = k
			if SetStart {
				return indexes[startIndex : endIndex+1]
			}
		}
	}
	return nil
}

//indexes is sequential
//if value is 2016, and indexs is [2015,2016,2017]
//the result is [2015,2016]
func makeLeList(value int, indexes []int) []int {
	for k, v := range indexes {
		if v == value {
			return indexes[:k+1]
		}
	}
	return nil
}

//indexes is sequential
//if value is 2016, and indexs is [2015,2016,2017,2018]
//the result is [2016,2017,2018]
func makeGeList(value int, indexes []int) []int {
	for k, v := range indexes {
		if v == value {
			return indexes[k:]
		}
	}
	return nil
}

func interList(l1 []int, l2 []int) []int {
	if len(l1) == 0 || len(l2) == 0 {
		return []int{}
	}

	l3 := make([]int, 0, len(l1)+len(l2))
	var i = 0
	var j = 0
	for i < len(l1) && j < len(l2) {
		if l1[i] == l2[j] {
			l3 = append(l3, l1[i])
			i++
			j++
		} else if l1[i] < l2[j] {
			i++
		} else {
			j++
		}
	}

	return l3
}

// l1 | l2
func unionList(l1 []int, l2 []int) []int {
	if len(l1) == 0 {
		return l2
	} else if len(l2) == 0 {
		return l1
	}

	l3 := make([]int, 0, len(l1)+len(l2))

	var i = 0
	var j = 0
	for i < len(l1) && j < len(l2) {
		if l1[i] < l2[j] {
			l3 = append(l3, l1[i])
			i++
		} else if l1[i] > l2[j] {
			l3 = append(l3, l2[j])
			j++
		} else {
			l3 = append(l3, l1[i])
			i++
			j++
		}
	}

	if i != len(l1) {
		l3 = append(l3, l1[i:]...)
	} else if j != len(l2) {
		l3 = append(l3, l2[j:]...)
	}

	return l3
}

// l1 - l2
func differentList(l1 []int, l2 []int) []int {
	if len(l1) == 0 {
		return []int{}
	} else if len(l2) == 0 {
		return l1
	}

	l3 := make([]int, 0, len(l1))

	var i = 0
	var j = 0
	for i < len(l1) && j < len(l2) {
		if l1[i] < l2[j] {
			l3 = append(l3, l1[i])
			i++
		} else if l1[i] > l2[j] {
			j++
		} else {
			i++
			j++
		}
	}

	if i != len(l1) {
		l3 = append(l3, l1[i:]...)
	}

	return l3
}
func cleanList(l []int) []int {
	s := make(map[int]struct{})
	listLen := len(l)
	l2 := make([]int, 0, listLen)

	for i := 0; i < listLen; i++ {
		k := l[i]
		s[k] = struct{}{}
	}
	for k := range s {
		l2 = append(l2, k)
	}
	return l2
}
