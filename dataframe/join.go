package dataframe

import (
	"fmt"
)

type Direction int

const (
	Left Direction = iota
	Right
	Inner
	Outer
)

type rowMatch int

const (
	matchLess rowMatch = iota - 1
	matchEq
	matchGreater
)

// Join two DataFrames into one new one. Works by sorting the DataFrames and
// merging the result, so essentially a MergeJoin.
func Join(opts ...JoinOpt) DataFrame {
	j := newJoin(opts...)

	left := j.leftSorted()
	right := j.rightSorted()

	compareAt := compareAcross(j, left, right)

	fmt.Println("Starting loop")

	var rStart, rIdx, lIdx int
	for lIdx < left.Nrow() {
		comparison := compareAt(lIdx, rIdx)

		if comparison == matchLess {
			lIdx++
			rIdx = rStart
		}

		fmt.Println(comparison, lIdx, rIdx)
		if comparison == matchEq {
			fmt.Println("Found a match!", comparison, lIdx, rIdx)
		}

		rIdx++
		if comparison == matchGreater {
			rStart = rIdx
		}

	}

	// TODO: This is obviously wrong
	return j.left
}

func compareAcross(j join, left, right DataFrame) func(int, int) rowMatch {
	leftKeyIndices := j.left.colIndexes(j.leftOn)
	rightKeyIndices := j.right.colIndexes(j.rightOn)

	return func(lIdx, rIdx int) rowMatch {
		for i := 0; i < len(leftKeyIndices); i++ {
			l := left.columns[leftKeyIndices[i]].Elem(lIdx)
			r := right.columns[rightKeyIndices[i]].Elem(rIdx)

			if l.Greater(r) {
				return 1
			} else if l.Less(r) {
				return -1
			}
		}

		return 0
	}
}

func (df DataFrame) colIndexes(cols []string) []int {
	indexes := make([]int, len(cols))
	for i, col := range cols {
		indexes[i] = df.colIndex(col)
	}
	return indexes
}
