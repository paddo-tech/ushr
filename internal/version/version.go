package version

import (
	"strconv"
	"strings"
)

var Version = "dev"

func Newer(target, current string) bool {
	parse := func(value string) ([3]uint64, bool) {
		var result [3]uint64
		core, _, _ := strings.Cut(strings.TrimPrefix(value, "v"), "-")
		core, _, _ = strings.Cut(core, "+")
		parts := strings.Split(core, ".")
		if len(parts) != 3 {
			return result, false
		}
		for i, part := range parts {
			n, err := strconv.ParseUint(part, 10, 64)
			if err != nil {
				return result, false
			}
			result[i] = n
		}
		return result, true
	}
	next, valid := parse(target)
	if !valid {
		return false
	}
	installed, valid := parse(current)
	if !valid {
		return true
	}
	for i := range next {
		if next[i] != installed[i] {
			return next[i] > installed[i]
		}
	}
	return strings.Contains(current, "-") && !strings.Contains(target, "-")
}
