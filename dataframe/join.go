package dataframe

import (
	//"fmt"
	"strings"
)

type Direction int

const (
	Left Direction = iota
	Right
	Inner
	Outer
)

// Join two DataFrames into one new one. Works by sorting the DataFrames and
// merging the result, so essentially a MergeJoin.
func Join(opts ...JoinOpt) DataFrame {
	j := newJoin(opts...)

	left := j.leftSorted()
	right := j.rightSorted()

	leftColumns := left.columns
	rightColumns := right.columns

	leftKeyIndices := df.colIndexes(j.leftOn)
	rightKeyIndices := df.colIndexes(j.rightOn)

	compareAt := func(lIdx, rIdx) int {
		lIdx
	}

	find := func(k key, start int) (begin, end int) {
		//fmt.Printf("Looking for %v in right\n", k)
		begin = -1
		end = -1

		for i := start; i < rightKeys.Nrow(); i++ {
			rKey := keyForRight(i)

			if k.eq(rKey) {
				end = i
				if begin == -1 {
					begin = i
				}
			} else if rKey.gt(k) {
				// Because these are ordered, we know that k can't possibly
				// exist in rightKeys if rKey is greater than k.
				return begin, i
			}
		}
		return
	}

	var rStart, rIdx int
	var lastKey key
	for lIdx := 0; lIdx < left.Nrow(); lIdx++ {
	}

	// TODO: This is obviously wrong
	return j.left
}
