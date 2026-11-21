package qpipe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pablo-botella/quevu"
)

// intake stands for a log: it keeps what it is given.
type intake struct {
	mu   sync.Mutex
	text strings.Builder
	fail error
}

func (i *intake) Write(p []byte) (int, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.fail != nil {
		return 0, i.fail
	}
	return i.text.Write(p)
}

func (i *intake) String() string {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.text.String()
}

func (i *intake) waitFor(t *testing.T, want string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for i.String() != want {
		if time.Now().After(deadline) {
			t.Fatalf("the log holds %q, want %q", i.String(), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

var _ quevu.Unit = (*Source)(nil)
var _ quevu.Exiter = (*Source)(nil)

func TestAStreamReachesTheLogAndEndsByItself(t *testing.T) {
	r, w := io.Pipe()
	var log intake
	s := From(r, &log)
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	// A program writes as it pleases: whole lines, pieces of lines.
	fmt.Fprint(w, "first line\nsecond ")
	fmt.Fprint(w, "line\n")
	log.waitFor(t, "first line\nsecond line\n")
	// The program ends: so does the input, without being told.
	w.Close()
	select {
	case err := <-s.Done():
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the input did not notice the end of its stream")
	}
	if s.Read() != uint64(len("first line\nsecond line\n")) {
		t.Fatalf("read %d bytes", s.Read())
	}
	// Stopping what already ended is nothing.
	if err := s.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestStopInterruptsABlockedRead(t *testing.T) {
	r, w := io.Pipe()
	defer w.Close()
	var log intake
	s := From(r, &log)
	s.Start(context.Background())
	fmt.Fprint(w, "taken\n")
	log.waitFor(t, "taken\n")
	// Nothing is being written; the read is blocked.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	// Stopped is not failed.
	if err := <-s.Done(); err != nil {
		t.Fatalf("a stop was reported as a failure: %v", err)
	}
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	// One stream, read once: a second run finds nothing and ends.
	<-s.Done()
}

func TestAStreamThatCannotBeClosedIsWaitedFor(t *testing.T) {
	release := make(chan struct{})
	reading := make(chan struct{})
	var log intake
	s := From(blocked{release, reading}, &log)
	s.Start(context.Background())
	<-reading
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := s.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	close(release)
	if err := s.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type blocked struct{ release, reading chan struct{} }

func (b blocked) Read([]byte) (int, error) {
	close(b.reading)
	<-b.release
	return 0, io.EOF
}

func TestALogThatRefusesIsAFailure(t *testing.T) {
	r, w := io.Pipe()
	defer w.Close()
	log := &intake{fail: errors.New("log closed")}
	s := From(r, log)
	s.Start(context.Background())
	go fmt.Fprint(w, "lost\n")
	select {
	case err := <-s.Done():
		if err == nil || !strings.Contains(err.Error(), "log closed") {
			t.Fatalf("got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the input kept reading into a log that takes nothing")
	}
}

func TestInAPanelItIsAUnitLikeAnyOther(t *testing.T) {
	r, w := io.Pipe()
	var log intake
	p := quevu.New()
	p.Add("program", From(r, &log))
	if err := p.StartAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	fmt.Fprint(w, "hello\n")
	log.waitFor(t, "hello\n")
	w.Close()
	// The program ended: the panel shows its input as stopped.
	deadline := time.Now().Add(3 * time.Second)
	for p.Units()[0].State != quevu.Stopped {
		if time.Now().After(deadline) {
			t.Fatalf("state %v", p.Units()[0].State)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// The real named endpoint of this system: a named pipe on Windows, a
// FIFO elsewhere. Programs write to it as to a file.
func TestNamedEndpoint(t *testing.T) {
	name := fmt.Sprintf("test-%d", time.Now().UnixNano())
	var log intake
	s, err := Listen(name, &log)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(context.Background()); err == nil {
		t.Fatal("started twice")
	}

	write := func(text string) {
		t.Helper()
		// What a redirection does: open the path for writing.
		var f *os.File
		var err error
		for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(10 * time.Millisecond) {
			if f, err = os.OpenFile(Endpoint(name), os.O_WRONLY, 0); err == nil || time.Now().After(deadline) {
				break
			}
		}
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprint(f, text)
		f.Close()
	}
	// One program writes and leaves; another comes after it.
	write("from the first\n")
	log.waitFor(t, "from the first\n")
	write("from the second\n")
	log.waitFor(t, "from the first\nfrom the second\n")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	// Stopped, the endpoint is gone.
	if f, err := os.OpenFile(Endpoint(name), os.O_WRONLY, 0); err == nil {
		f.Close()
		t.Fatal("the endpoint outlived its source")
	}
	// And it comes back with the next start.
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	write("after a restart\n")
	log.waitFor(t, "from the first\nfrom the second\nafter a restart\n")
	s.Stop(ctx)

	if _, err := Listen("bad name!", &log); err == nil {
		t.Fatal("bad name accepted")
	}
}
