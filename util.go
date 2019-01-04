package gofakes3

import "strconv"

func parseClampedInt(in string, defaultValue, min, max int64) (int64, error) {
	if in == "" {
		return defaultValue, nil
	}

	v, err := strconv.ParseInt(in, 10, 0)
	if err != nil {
		return defaultValue, err
	}

	if v < min {
		v = min
	} else if v > max {
		v = max
	}
	return v, nil
}
