package qpipe

import (
	"errors"
	"io"
	"net"
	"sync"

	"github.com/Microsoft/go-winio"
)

// Endpoint returns the path of the named pipe behind a name. A program
// sends its output there with a redirection:
//
//	program > \\.\pipe\quevu-in-NAME
func Endpoint(name string) string { return `\\.\pipe\quevu-in-` + name }

func listen(name string, into io.Writer) (*Source, error) {
	var (
		mu sync.Mutex
		l  net.Listener
	)
	s := &Source{into: into}
	s.prepare = func() error {
		// A byte stream, as an anonymous pipe is. By default a pipe is
		// reachable by its creator, administrators and the system.
		nl, err := winio.ListenPipe(Endpoint(name), &winio.PipeConfig{MessageMode: false})
		if err != nil {
			return err
		}
		mu.Lock()
		l = nl
		mu.Unlock()
		return nil
	}
	s.release = func() {
		mu.Lock()
		if l != nil {
			l.Close()
			l = nil
		}
		mu.Unlock()
	}
	s.open = func() (io.ReadCloser, error) {
		mu.Lock()
		cur := l
		mu.Unlock()
		if cur == nil {
			return nil, io.EOF
		}
		// One writer at a time: the next one waits for this one to go.
		c, err := cur.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil, io.EOF
			}
			return nil, err
		}
		return c, nil
	}
	return s, nil
}
