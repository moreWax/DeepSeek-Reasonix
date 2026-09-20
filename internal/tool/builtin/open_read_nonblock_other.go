//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris && !windows

package builtin

import "os"

func openReadFile(path string) (*os.File, error) {
	return os.Open(path)
}
