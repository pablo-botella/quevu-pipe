//go:build !(windows || linux || darwin || freebsd || netbsd || openbsd)

package qpipe

import (
	"errors"
	"io"
)

// Endpoint has no meaning on this system.
func Endpoint(name string) string { return "" }

func listen(string, io.Writer) (*Source, error) {
	return nil, errors.New("qpipe: no named pipes on this system; use From")
}
