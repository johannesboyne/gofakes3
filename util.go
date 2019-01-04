package gofakes3

import "strconv"

func parseClampedInt(in string, defaultValue, min, max int64) (int64, error) {
	var v int64
	if in == "" {
		v = defaultValue
	} else {
		var err error
		v, err = strconv.ParseInt(in, 10, 0)
		if err != nil {
			return defaultValue, err
		}
	}

	if v < min {
		v = min
	} else if v > max {
		v = max
	}

	return v, nil
}
