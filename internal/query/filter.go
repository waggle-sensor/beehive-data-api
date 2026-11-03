// filter.go allowslists the characters a filter field or pattern may contain
// before a backend interpolates it into hot or cold.

package query

import "regexp"

var validQueryStringRE = regexp.MustCompile("^[A-Za-z0-9+-_.*:| ]*$")

// IsValidFilterString reports whether s can be interpolated into a query.
func IsValidFilterString(s string) bool {
	return validQueryStringRE.MatchString(s)
}
