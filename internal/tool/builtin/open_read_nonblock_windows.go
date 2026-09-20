//go:build windows

package builtin

import "os"

func openReadFile(path string) (*os.File, error) {
	return os.Open(path)
}
