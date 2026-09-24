package schema

import "regexp"

func identRE() *regexp.Regexp {
	return regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
}
