// An HTTP server that keeps answering while it computes.
//
//   GET /primes?below=N              counts the primes below N on goroutines, in parallel
//   GET /primes?below=N&on=main      counts them on the main thread, which then answers nothing else
//   GET /ping                        answers "pong"
//
//   bun server.ts            serves on port 3000 (or $PORT)
//   bun server.ts --demo     sends /primes requests and measures how long /ping takes meanwhile

import { parseArgs } from "node:util";
import { countPrimes } from "./primes.ts";

const chunks = navigator.hardwareConcurrency;

async function primesBelow(below: number, onMain: boolean) {
  if (onMain) return countPrimes(0, below);
  // One goroutine per chunk. Bun.go() runs the export of primes.ts on the pool's threads.
  const size = Math.ceil(below / chunks);
  const counts = await Promise.all(
    Array.from({ length: chunks }, (_, i) => Bun.go(countPrimes, i * size, Math.min((i + 1) * size, below))),
  );
  return counts.reduce((a, b) => a + b, 0);
}

function serve(port: number) {
  return Bun.serve({
    port,
    routes: {
      "/ping": new Response("pong"),
      "/primes": async req => {
        const params = new URL(req.url).searchParams;
        const below = Number(params.get("below") ?? 1_000_000);
        if (!Number.isSafeInteger(below) || below < 0) return new Response("bad ?below", { status: 400 });
        return Response.json({ below, primes: await primesBelow(below, params.get("on") === "main") });
      },
    },
  });
}

const { values } = parseArgs({
  options: {
    demo: { type: "boolean", default: false },
    below: { type: "string", default: "3000000" },
    requests: { type: "string", default: "4" },
  },
});

if (!values.demo) {
  const server = serve(Number(process.env.PORT ?? 3000));
  console.log(`Listening on ${server.url}`);
} else {
  // Send `requests` /primes requests at once, and /ping the server until they are answered.
  using server = serve(0);
  const requests = Number(values.requests);
  const n = (value: number) => value.toLocaleString("en-US");
  console.log(`${requests} requests for the primes below ${n(Number(values.below))}, and /ping meanwhile:`);

  for (const on of ["goroutines", "main"]) {
    const url = new URL(`/primes?below=${values.below}&on=${on}`, server.url);
    const start = performance.now();
    let answered = false;
    const answers = Promise.all(Array.from({ length: requests }, () => fetch(url).then(res => res.json()))).finally(
      () => (answered = true),
    );

    let pings = 0;
    let slowest = 0;
    while (!answered) {
      const sent = performance.now();
      await fetch(new URL("/ping", server.url)).then(res => res.text());
      slowest = Math.max(slowest, performance.now() - sent);
      pings++;
    }
    const [{ primes }] = await answers;
    const total = performance.now() - start;
    console.log(
      `  on ${on.padEnd(10)}  ${n(primes)} primes   all answered in ${n(Math.round(total))} ms   ` +
        `${pings} pings, slowest ${n(Math.round(slowest))} ms`,
    );
  }
}
