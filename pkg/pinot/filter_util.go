package pinot

// GetInOrNotInFromOperator maps a filter operator to its IN/NOT IN counterpart for subquery filters.
func GetInOrNotInFromOperator(op FilterOperator) string {
	switch op {
	case FilterOpNotIn, FilterOpNotEquals:
		return "not in"
	default:
		return "in"
	}
}
