// Package qpipe is an input of quevu: it reads what a program writes to
// its standard output, through a pipe, and feeds it to a log, so the data
// follows the normal way from there to the writers.
//
// It sits at the input end and does one thing: read a stream and hand it
// over. It does not launch programs, does not look at standard error and
// writes nothing back. It is not a provider either: nothing can be asked
// of it.
//
// A pipe is whatever each system calls one. Two ways to get it:
//
//   - From takes a stream that is already open: the read end of a pipe a
//     parent holds, or the standard input of a process placed at the end
//     of a shell pipeline.
//   - Listen opens a named endpoint that programs write to: a named pipe
//     on Windows, a FIFO elsewhere. A program reaches it by redirecting
//     its standard output there.
//
// Either one is a qkernel.Unit, so it is started and stopped with the rest
// of the application, and it ends by itself when its stream does.
package qpipe

import (
	"errors"
	"fmt"
	"io"
	"regexp"
	"sync"
	"sync/atomic"

	"context"
)

// Source reads a stream and feeds it to a log. "Source" is a working
// name: the piece is an input, and has nothing to do with qkernel.Source,
// which is what a console shows.
type Source struct {
	into io.Writer
	// open returns the next stream to read. io.EOF means there will be
	// no more.
	open func() (io.ReadCloser, error)
	// prepare and release bracket a run, for inputs that hold something
	// between streams (a listening endpoint).
	prepare func() error
	release func()

	mu      sync.Mutex
	running bool
	stopped bool
	cur     io.Closer
	fin     chan struct{}
	done    chan error
	err     error

	read atomic.Uint64
}

// From reads one stream until it ends. into is the log: every line read
// becomes a chunk of it.
//
// If r can be closed, Stop closes it, which is what makes a blocked read
// return. A reader that cannot be closed cannot be interrupted: Stop then
// waits for the stream to end by itself, for as long as its context says.
func From(r io.Reader, into io.Writer) *Source {
	rc, ok := r.(io.ReadCloser)
	if !ok {
		rc = io.NopCloser(r)
	}
	used := false
	return &Source{
		into: into,
		open: func() (io.ReadCloser, error) {
			if used {
				return nil, io.EOF
			}
			used = true
			return rc, nil
		},
	}
}

var nameRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// Listen prepares a named endpoint that programs write to, and reads
// whatever arrives there, one writer after another, until it is stopped.
// The endpoint exists while the source is running. name is letters,
// digits, dot, dash and underscore, up to 64; Endpoint(name) is the path
// a program redirects its output to.
func Listen(name string, into io.Writer) (*Source, error) {
	if !nameRE.MatchString(name) {
		return nil, fmt.Errorf("qpipe: invalid endpoint name %q (letters, digits, dot, dash and underscore, up to 64)", name)
	}
	return listen(name, into)
}

// Start begins reading. It returns at once; the reading goes on in the
// background.
func (s *Source) Start(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return errors.New("qpipe: already running")
	}
	if s.prepare != nil {
		if err := s.prepare(); err != nil {
			return err
		}
	}
	s.running, s.stopped, s.err = true, false, nil
	s.fin = make(chan struct{})
	s.done = make(chan error, 1)
	go s.run(s.fin, s.done)
	return nil
}

func (s *Source) run(fin chan struct{}, done chan error) {
	err := s.pump()
	s.mu.Lock()
	if s.stopped {
		// Being stopped is not a failure, whatever the read said when
		// its stream was closed under it.
		err = nil
	}
	s.running, s.cur, s.err = false, nil, err
	release := s.release
	s.mu.Unlock()
	if release != nil {
		release()
	}
	done <- err
	close(done)
	close(fin)
}

// pump reads stream after stream until there are no more.
func (s *Source) pump() error {
	buf := make([]byte, 32<<10)
	for {
		rc, err := s.open()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		s.mu.Lock()
		if s.stopped {
			s.mu.Unlock()
			rc.Close()
			return nil
		}
		s.cur = rc
		s.mu.Unlock()

		for {
			n, rerr := rc.Read(buf)
			if n > 0 {
				s.read.Add(uint64(n))
				if _, werr := s.into.Write(buf[:n]); werr != nil {
					rc.Close()
					return fmt.Errorf("qpipe: the log did not take the data: %w", werr)
				}
			}
			if rerr != nil {
				break
			}
		}
		rc.Close()
		// A stream that ends, cleanly or not, is the writer going away.
		// What matters is whether another one may come.
	}
}

// Stop ends the reading in an orderly way: what has been read is already
// in the log, and nothing more is taken. It returns when the reading has
// stopped, or when ctx says to wait no longer.
func (s *Source) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.stopped = true
	cur, fin, release := s.cur, s.fin, s.release
	s.mu.Unlock()
	// Closing what is being read, and what streams arrive at, is what
	// makes a blocked read or a blocked wait return.
	if cur != nil {
		cur.Close()
	}
	if release != nil {
		release()
	}
	select {
	case <-fin:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Done yields the result of the current run when it ends, so a panel
// notices an input whose stream ended without anybody stopping it.
func (s *Source) Done() <-chan error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done
}

// Read returns how many bytes have been read since the source was made.
func (s *Source) Read() uint64 { return s.read.Load() }
