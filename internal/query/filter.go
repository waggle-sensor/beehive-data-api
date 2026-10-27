// filter.go allowslists the characters a filter field or pattern may contain before a backend interpolates it into hot or cold.
package query

import "regexp"

var validQueryStringRE = regexp.MustCompile("^[A-Za-z0-9+-_.*:| ]*$")

// IsValidFilterString checks if the string is a valid filter string for the filter.
func IsValidFilterString(s string) bool {
	return validQueryStringRE.MatchString(s)
}
