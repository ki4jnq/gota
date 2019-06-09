package dataframe

type join struct {
	direction Direction

	leftOn  []string
	rightOn []string

	left  DataFrame
	right DataFrame

	sorted bool
}

type JoinOpt func(join) join

func newJoin(opts ...JoinOpt) join {
	return join{}.with(opts...)
}

func (j join) with(opts ...JoinOpt) join {
	for _, opt := range opts {
		j = opt(j)
	}

	return j
}

func On(cols ...string) JoinOpt {
	return func(j join) join {
		return j.with(
			LeftOn(cols...),
			RightOn(cols...),
		)
	}
}

func LeftOn(cols ...string) JoinOpt {
	return func(j join) join {
		j.leftOn = cols
		return j
	}
}

func RightOn(cols ...string) JoinOpt {
	return func(j join) join {
		j.rightOn = cols
		return j
	}
}

func LeftFrame(left DataFrame) JoinOpt {
	return func(j join) join {
		j.left = left
		return j
	}
}

func RightFrame(right DataFrame) JoinOpt {
	return func(j join) join {
		j.right = right
		return j
	}
}

func AssumeSorted(sorted bool) JoinOpt {
	return func(j join) join {
		j.sorted = sorted
		return j
	}
}

func (j join) leftSorted() DataFrame {
	return j.sort(j.left, j.leftOn)
}

func (j join) rightSorted() DataFrame {
	return j.sort(j.right, j.rightOn)
}

// sort returns the dataframe after sorting it by the columns specified. Does
// nothing if `AssumeSorted` was set to true.
func (j join) sort(df DataFrame, cols []string) DataFrame {
	if j.sorted {
		return df
	}

	var sorts []Order
	for _, col := range cols {
		sorts = append(sorts, Order{col, false})
	}

	return df.Arrange(sorts...)
}

// buildJoinFrame builds an empty DataFrame to hold the result of joining left
// and right.
func (j join) buildJoinFrame() DataFrame {
	df := DataFrame{}

	for _, col := range j.left.columns {
		df.columns = append(df.columns, col)
		df.ncols++
	}

	for _, col := range j.right.columns {
		df.columns = append(df.columns, col)
		df.ncols++
	}

	return df
}
