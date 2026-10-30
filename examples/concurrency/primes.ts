// The CPU work of server.ts: count the primes in [from, to) by trial division.
//
// A function a module exports is the other kind of function Bun.go() runs: each pool thread
// imports the module once, then calls the export. `bun build` finds it too.

export function countPrimes(from: number, to: number): number {
  let count = 0;
  for (let n = from; n < to; n++) if (isPrime(n)) count++;
  return count;
}

function isPrime(n: number) {
  if (n < 2) return false;
  if (n % 2 === 0) return n === 2;
  for (let d = 3; d * d <= n; d += 2) if (n % d === 0) return false;
  return true;
}
