package dataframe

import ()

type Direction int

const (
	Left Direction = iota
	Right
	Inner
	Outer
)

// key wraps a 1 x N dataframe that uniquely identifies the data we want to
// join across both dataframes.
type key struct {
	DataFrame
}

// Join two DataFrames into one new one. Works by sorting the DataFrames and
// merging the result, so essentially a MergeJoin.
func Join(opts ...JoinOpt) DataFrame {
	j := newJoin(opts...)

	left := j.leftSorted()
	right := j.rightSorted()
	leftKeys := left.Select(j.leftOn)
	rightKeys := right.Select(j.rightOn)

	// Find key for the row identified by idx on the left hand side. A key for
	// our purposes is a dataframe with one row and j.LeftOn columns.
	keyForLeft := func(idx int) key {
		return key{leftKeys.Subset(idx)}
	}

	keyForRight := func(idx int) key {
		return key{rightKeys.Subset(idx)}
	}

	find := func(k key, start int) (begin, end int) {
		begin = -1
		end = -1

		for i := start; i < rightKeys.Nrow(); i++ {
			rKey := keyForRight(i)

			if k.eq(rKey) {
				end = i
				if begin == -1 {
					begin = i
				}
			} else if k.lt(rKey) {

			}
		}
		return
	}

	var rStart, rIdx int
	var lastKey key
	for lIdx := 0; lIdx < left.Nrow(); lIdx++ {
		k := keyForLeft(lIdx)

		firstMatch, lastMatch := find(k, rStart)

		// No Match found.
		if firstMatch == -1 {
		}

		// The rest of the dataframe matched.
		if lastMatch == -1 {
		}

		if !lastKey.eq(k) {
			rStart = rIdx
		}
	}

	// TODO: This is obviously wrong
	return leftKeys
}

// Compare each element of two series of equal length.
func (k key) compare(o key) (result int) {
	for i := 0; i < k.Ncol() && i < o.Ncol(); i++ {
		left := k.Elem(0, i)
		right := o.Elem(0, i)

		if left.Eq(right) {
			continue
		} else if left.Greater(right) {
			return 1
		} else if left.Less(right) {
			return -1
		}
	}

	return
}

func (k key) eq(o key) bool {
	r := k.compare(o)
	return r == 0
}

func (k key) lt(o key) bool {
	r := k.compare(o)
	return r < 0
}

func (k key) lte(o key) bool {
	r := k.compare(o)
	return r <= 0
}

func (k key) gt(o key) bool {
	r := k.compare(o)
	return r > 0
}

func (k key) gte(o key) bool {
	r := k.compare(o)
	return r >= 0
}
