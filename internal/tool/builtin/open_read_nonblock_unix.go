//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package builtin

import (
	"os"
	"syscall"
)

// openReadFile uses non-blocking open so a path swapped to a FIFO or device
// cannot strand a speculative read before the opened handle can be classified.
func openReadFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
}
