module github.com/pablo-botella/quevu-pipe

go 1.26.0

require (
	github.com/Microsoft/go-winio v0.6.4
	github.com/pablo-botella/quevu-kernel v0.0.0
)

require golang.org/x/sys v0.49.0 // indirect

replace github.com/pablo-botella/quevu-kernel => ../quevu-kernel
