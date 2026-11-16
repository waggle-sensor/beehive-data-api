// duration.go resolves one query's relative bounds before tier routing.
// The same UTC instant is used for both bounds, and the caller's query is left unchanged.

package tiered

import (
	"fmt"
	"math"
	"time"

	model "beehive-data-api/internal/query"
)

// fluxDuration is a Flux duration: calendar months plus a fixed nanosecond offset.
// Years are twelve months. Days and weeks are exact 24-hour units.
type fluxDuration struct {
	months int64
	nanos  int64
}

// resolveRelativeBounds returns a query whose relative bounds are absolute RFC3339
// timestamps. Absolute bounds and an omitted bound stay as the caller wrote them.
// Both bounds are resolved from the same instant.
func resolveRelativeBounds(query *model.Query, now time.Time) (*model.Query, error) {
	start, startRelative, err := resolveBound(query.Start, now)
	if err != nil {
		return nil, fmt.Errorf("invalid start timestamp %q: %w", query.Start, err)
	}
	end, endRelative, err := resolveBound(query.End, now)
	if err != nil {
		return nil, fmt.Errorf("invalid end timestamp %q: %w", query.End, err)
	}
	if !startRelative && !endRelative {
		return query, nil
	}
	resolved := *query
	if startRelative {
		resolved.Start = start.Format(time.RFC3339Nano)
	}
	if endRelative {
		resolved.End = end.Format(time.RFC3339Nano)
	}
	return &resolved, nil
}

// resolveBound reports whether s is a Flux duration and, when it is, the absolute instant.
// A string that is neither RFC3339 nor a duration is left for the absolute parser.
func resolveBound(s string, now time.Time) (time.Time, bool, error) {
	if s == "" {
		return time.Time{}, false, nil
	}
	if _, err := time.Parse(time.RFC3339, s); err == nil {
		return time.Time{}, false, nil
	}
	duration, err := parseFluxDuration(s)
	if err != nil {
		if s[0] == '+' || s[0] == '-' {
			return time.Time{}, false, err
		}
		return time.Time{}, false, nil
	}
	instant, err := addFluxDuration(now.UTC(), duration)
	if err != nil {
		return time.Time{}, false, err
	}
	return instant, true, nil
}

// parseFluxDuration parses one Flux duration literal, including a leading sign.
// The sign applies to every unit. Units may repeat and may appear in any order.
func parseFluxDuration(s string) (fluxDuration, error) {
	if s == "" {
		return fluxDuration{}, fmt.Errorf("invalid duration")
	}
	sign := int64(1)
	if s[0] == '+' || s[0] == '-' {
		if s[0] == '-' {
			sign = -1
		}
		s = s[1:]
	}
	if s == "" {
		return fluxDuration{}, fmt.Errorf("invalid duration")
	}
	var parsed fluxDuration
	for len(s) > 0 {
		i := 0
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
		}
		if i == 0 {
			return fluxDuration{}, fmt.Errorf("invalid duration")
		}
		magnitude, err := parseSignedMagnitude(s[:i], sign)
		if err != nil {
			return fluxDuration{}, err
		}
		s = s[i:]
		unit, rest, ok := consumeDurationUnit(s)
		if !ok {
			return fluxDuration{}, fmt.Errorf("invalid duration")
		}
		s = rest
		if err = parsed.addUnit(magnitude, unit); err != nil {
			return fluxDuration{}, err
		}
	}
	return parsed, nil
}

// parseSignedMagnitude applies the duration sign without wrapping.
func parseSignedMagnitude(digits string, sign int64) (int64, error) {
	var magnitude int64
	for _, c := range digits {
		next := magnitude*10 + int64(c-'0')
		if magnitude > math.MaxInt64/10 || next < magnitude {
			return 0, fmt.Errorf("invalid duration")
		}
		magnitude = next
	}
	if sign < 0 {
		magnitude = -magnitude
	}
	return magnitude, nil
}

// consumeDurationUnit takes the longest Flux unit prefix.
// Longer names are listed first so "mo" is not read as "m".
func consumeDurationUnit(s string) (string, string, bool) {
	for _, unit := range []string{"ns", "us", "µs", "ms", "mo", "m", "h", "d", "w", "s", "y"} {
		if len(s) >= len(unit) && s[:len(unit)] == unit {
			return unit, s[len(unit):], true
		}
	}
	return "", s, false
}

// addUnit adds one signed magnitude in the named unit.
func (d *fluxDuration) addUnit(magnitude int64, unit string) error {
	switch unit {
	case "y":
		months, err := multiply(magnitude, 12)
		if err != nil {
			return err
		}
		return d.addMonths(months)
	case "mo":
		return d.addMonths(magnitude)
	case "w":
		return d.addNanos(magnitude, 7*24*int64(time.Hour))
	case "d":
		return d.addNanos(magnitude, 24*int64(time.Hour))
	case "h":
		return d.addNanos(magnitude, int64(time.Hour))
	case "m":
		return d.addNanos(magnitude, int64(time.Minute))
	case "s":
		return d.addNanos(magnitude, int64(time.Second))
	case "ms":
		return d.addNanos(magnitude, int64(time.Millisecond))
	case "us", "µs":
		return d.addNanos(magnitude, int64(time.Microsecond))
	case "ns":
		return d.addNanos(magnitude, int64(time.Nanosecond))
	default:
		return fmt.Errorf("invalid duration")
	}
}

// addMonths accumulates calendar months, rejecting a product that wraps.
func (d *fluxDuration) addMonths(months int64) error {
	if months == 0 {
		return nil
	}
	sum := d.months + months
	if (months > 0 && sum < d.months) || (months < 0 && sum > d.months) {
		return fmt.Errorf("invalid duration")
	}
	d.months = sum
	return nil
}

// multiply returns magnitude*unit, rejecting a product that wraps.
func multiply(magnitude, unit int64) (int64, error) {
	if magnitude == 0 || unit == 0 {
		return 0, nil
	}
	product := magnitude * unit
	if product/unit != magnitude {
		return 0, fmt.Errorf("invalid duration")
	}
	return product, nil
}

// addNanos accumulates magnitude*unit nanoseconds, rejecting a product or sum that wraps.
func (d *fluxDuration) addNanos(magnitude, unit int64) error {
	product, err := multiply(magnitude, unit)
	if err != nil || product == 0 {
		return err
	}
	sum := d.nanos + product
	if (product > 0 && sum < d.nanos) || (product < 0 && sum > d.nanos) {
		return fmt.Errorf("invalid duration")
	}
	d.nanos = sum
	return nil
}

// addFluxDuration applies calendar months first, then the fixed offset.
// A day past the end of the destination month clamps to that month's last day.
func addFluxDuration(t time.Time, d fluxDuration) (time.Time, error) {
	t = t.UTC()
	if d.months != 0 {
		var err error
		t, err = addCalendarMonths(t, d.months)
		if err != nil {
			return time.Time{}, err
		}
	}
	if d.nanos == 0 {
		return t, nil
	}
	var instant time.Time
	var panicked bool
	func() {
		defer func() {
			if recover() != nil {
				panicked = true
			}
		}()
		instant = t.Add(time.Duration(d.nanos))
	}()
	if panicked || (d.nanos > 0 && !instant.After(t)) || (d.nanos < 0 && !instant.Before(t)) {
		return time.Time{}, fmt.Errorf("duration overflows the time range")
	}
	return instant, nil
}

// addCalendarMonths moves t by whole calendar months.
func addCalendarMonths(t time.Time, months int64) (time.Time, error) {
	index := int64(t.Month()) - 1 + months
	year := int64(t.Year())
	if index >= 0 {
		year += index / 12
		index = index % 12
	} else {
		year += (index+1)/12 - 1
		index = index % 12
		if index < 0 {
			index += 12
		}
	}
	if year < 1 || year > 9999 {
		return time.Time{}, fmt.Errorf("duration overflows the time range")
	}
	month := time.Month(index + 1)
	day := t.Day()
	last := time.Date(int(year), month+1, 0, 0, 0, 0, 0, time.UTC).Day()
	if day > last {
		day = last
	}
	return time.Date(int(year), month, day, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC), nil
}
