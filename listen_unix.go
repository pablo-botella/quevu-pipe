//go:build linux || darwin || freebsd || netbsd || openbsd

package qpipe

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// Endpoint returns the path of the FIFO behind a name: in the runtime
// directory of the user when there is one, in the temporary directory
// otherwise. A program sends its output there with a redirection:
//
//	program > "$XDG_RUNTIME_DIR/quevu-in-NAME.fifo"
func Endpoint(name string) string {
	dir := os.Getenv("XDG_RUNTIME_DIR")
	if dir == "" {
		dir = os.TempDir()
	}
	return filepath.Join(dir, "quevu-in-"+name+".fifo")
}

func listen(name string, into io.Writer) (*Source, error) {
	path := Endpoint(name)
	var (
		mu   sync.Mutex
		f    *os.File
		used bool
	)
	s := &Source{into: into}
	s.prepare = func() error {
		// Only the user that runs the application may write to it.
		if err := syscall.Mkfifo(path, 0o600); err != nil && !errors.Is(err, syscall.EEXIST) {
			return err
		}
		// Opened for reading and writing: the FIFO then never reports
		// its end when a writer leaves, so writers come and go and the
		// stream is one. Only closing it ends the reading.
		nf, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			return err
		}
		mu.Lock()
		f, used = nf, false
		mu.Unlock()
		return nil
	}
	s.release = func() {
		mu.Lock()
		if f != nil {
			f.Close()
			f = nil
			os.Remove(path)
		}
		mu.Unlock()
	}
	s.open = func() (io.ReadCloser, error) {
		mu.Lock()
		defer mu.Unlock()
		if f == nil || used {
			return nil, io.EOF
		}
		used = true
		// The source closes what it reads; closing a duplicate here
		// would leave the real one open, so the file itself is handed.
		return f, nil
	}
	return s, nil
}
