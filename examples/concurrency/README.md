# Goroutines and channels

Two programs that use `Bun.go`, `Bun.Channel` and `Bun.select`. The guide is [docs/runtime/concurrency.mdx](../../docs/runtime/concurrency.mdx).

```sh
# Count the primes below 20 million on a goroutine per core, then on the main thread alone.
bun pipeline.ts
bun pipeline.ts --workers 4 --limit 50000000
bun pipeline.ts --timeout 100    # stop after 100 ms: an AbortSignal and a quit channel

# A Bun.serve() server that counts primes on goroutines and stays responsive.
bun server.ts                    # http://localhost:3000/primes?below=1000000
bun server.ts --demo             # measure /ping while /primes requests compute

# Both also run as executables.
bun build --compile pipeline.ts --outfile pipeline && ./pipeline
```

`GOMAXPROCS` sets the size of the goroutine pool (2 or more; the default is the number of cores).
