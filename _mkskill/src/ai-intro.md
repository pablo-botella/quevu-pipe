---
mkskill:
  pos: 5
  in: ai*
---

# quevu-pipe - agent notes

Go package `github.com/pablo-botella/quevu-pipe`, imported as `qpipe`: an
input that reads a stream and writes it to a log. One public type,
`qpipe.Source` (a `qkernel.Unit` and `qkernel.Exiter`), from `From` or
`Listen`. Depends on `quevu` for the contracts used in tests and, on
Windows, on `github.com/Microsoft/go-winio`.

Scope set by the spec: standard output only. No standard error, no
standard input, no launching of programs. Do not add them here without
being asked.
