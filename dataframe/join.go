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

	var rStart, rIdx, lIdx int

	for lIdx < left.nrows {
		switch compareAt(lIdx, rIdx) {
		case matchLess:
			lIdx++
			rIdx = rStart - 1
		case matchEq:
			add(lIdx, rIdx)
		case matchGreater:
			rStart = rIdx
		}

		if rIdx < right.Nrow()-1 {
			rIdx++
		} else {
			// TODO: I'm not sure that this is correct...
			lIdx++
		}
	}

	return final
}

func compareAcross(j join, left, right DataFrame) func(int, int) rowMatch {
	leftKeyIndices := left.colIndexes(j.leftOn)
	rightKeyIndices := right.colIndexes(j.rightOn)

	return func(lIdx, rIdx int) rowMatch {
		var i int
		defer func() {
			if e := recover(); e != nil {
				fmt.Println("Panic in compareAcross", lIdx, rIdx, i)
				fmt.Println(right.columns[rightKeyIndices[i]].Len())
				panic(e)
			}
		}()

		for i = 0; i < len(leftKeyIndices); i++ {
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
		for i := range final.columns {
			var e series.Element

			if i < left.ncols {
				e = left.columns[i].Elem(lIdx)
			}
			if i >= left.ncols {
				e = right.columns[i-left.ncols].Elem(rIdx)
			}

			final.columns[i].Append(e)
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
