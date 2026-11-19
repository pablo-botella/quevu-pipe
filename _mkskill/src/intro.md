---
mkskill:
  pos: 10
  in: readme
---

# quevu-pipe

An input of [quevu](https://github.com/pablo-botella/quevu): it reads what
a program writes to its standard output, through a pipe, and feeds it to a
log. From there the data follows the normal way: the log sends it to its
consumers.

```go
// A named endpoint that programs write to...
in, _ := qpipe.Listen("billing", theLog)
panel.Add("billing output", in)

// ...or a stream that is already open.
in := qpipe.From(os.Stdin, theLog)
```

```
program > \\.\pipe\quevu-in-billing            (Windows)
program > "$XDG_RUNTIME_DIR/quevu-in-billing.fifo"   (elsewhere)
```
