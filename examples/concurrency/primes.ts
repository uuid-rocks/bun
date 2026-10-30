// The CPU work of the examples: count the primes in [from, to) by trial division.
//
// A module function is the other form Bun.go() runs: each pool thread imports this module once,
// then calls the export.

export function countPrimes(from: number, to: number, signal?: AbortSignal): number {
  let count = 0;
  for (let n = Math.max(from, 2); n < to; n++) {
    // Nothing interrupts a goroutine that computes: it has to look at its signal.
    if (n % 16_384 === 0 && signal?.aborted) return -1;
    if (n > 2 && n % 2 === 0) continue;
    let prime = true;
    for (let d = 3; d * d <= n; d += 2) {
      if (n % d === 0) {
        prime = false;
        break;
      }
    }
    if (prime) count++;
  }
  return count;
}
