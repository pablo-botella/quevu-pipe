---
mkskill:
  pos: 100
  in: "*"
---

## What it is

It sits at the input end and does one thing: read a stream and hand it to
a log. It is not a provider: nothing can be asked of it. It does not
launch programs, does not look at standard error and writes nothing back.

"Source" is a working name for the piece. It has nothing to do with
`qkernel.Source`, which is what a console shows.

## Getting the pipe

A pipe is whatever each system calls one.

- **`From(r, log)`** takes a stream that is already open: the read end of
  a pipe a parent holds, or the standard input of a process placed at the
  end of a shell pipeline. It reads until the stream ends.
- **`Listen(name, log)`** prepares a named endpoint that programs write
  to, and reads whatever arrives, one writer after another, until it is
  stopped. On Windows it is a named pipe, elsewhere a FIFO;
  `Endpoint(name)` is the path a program redirects its output to. The
  endpoint exists only while the source is running.

`log` is anything that takes a stream of text and makes chunks of its
lines: a `qkernel.QHub`.

## As a unit

A source is a `qkernel.Unit`, so it starts and stops with the rest of the
application, in a panel:

- `Start` returns at once; reading goes on in the background.
- `Stop` takes nothing more and returns when reading has stopped. What was
  read is already in the log.
- A source whose stream ends stops by itself, and the panel shows it.
- If the log refuses the data, the source ends with that error and shows
  as failed.

## Rules and gotchas

- **One writer at a time on a named endpoint.** The next one waits until
  the current one closes.
- **A stream that cannot be closed cannot be interrupted.** `Stop` closes
  what is being read to make a blocked read return. If the reader given to
  `From` is not an `io.Closer`, `Stop` waits for the stream to end, for as
  long as its context allows.
- **Lines, not messages.** What is read is passed to the log as it comes;
  the log makes a chunk of every line. An unfinished last line stays with
  the log until the rest arrives or the log is closed.
- **Local, and for the same user.** The endpoint is reachable by the user
  that created it.
- **Not run on Linux or macOS.** The FIFO side builds for them; only the
  Windows named pipe has been run, in tests and with real programs
  redirected from the shell.
