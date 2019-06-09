package dataframe

import (
	"fmt"

	"github.com/ki4jnq/gota/series"
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
	final := j.buildJoinFrame()

	compareAt := compareAcross(j, left, right)
	add := addRows(left, right, &final)

	fmt.Println("Starting loop")

	var matchCount int
	var rStart, rIdx, lIdx int
	for lIdx < left.Nrow() {
		comparison := compareAt(lIdx, rIdx)

		if comparison == matchLess {
			lIdx++
			rIdx = rStart
		}

		if comparison == matchEq {
			matchCount++
			//fmt.Println("Adding a row", lIdx, rIdx, matchCount)
			add(lIdx, rIdx)
		}

		if comparison == matchGreater {
			rStart = rIdx
		}

		if rIdx < right.Nrow() {
			rIdx++
		}
	}

	// TODO: This is obviously wrong
	return final
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

func addRows(left, right DataFrame, final *DataFrame) func(int, int) {
	return func(lIdx, rIdx int) {
		for i, col := range final.columns {
			var e series.Element

			if i < left.ncols {
				e = left.columns[i].Elem(lIdx)
			}
			if i >= left.ncols {
				e = right.columns[i-left.ncols].Elem(rIdx)
			}

			col.Append(e)
		}
		final.nrows++
	}
}

func (df DataFrame) colIndexes(cols []string) []int {
	indexes := make([]int, len(cols))
	for i, col := range cols {
		indexes[i] = df.colIndex(col)
	}
	return indexes
}
