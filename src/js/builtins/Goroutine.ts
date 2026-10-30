// The microtask a Bun.go goroutine starts on (BunGo.cpp's `run`): fn(...args), then
// settled(true, result) or settled(false, what it threw). A private builtin's frame shows in no
// stack trace, and nothing of the pool's runtime is under it.
$visibility = "Private";
export function runGoroutine(fn: Function, args: unknown[], settled: Function) {
  let result;
  try {
    result = fn.$apply(undefined, args);
  } catch (error) {
    settled.$call(undefined, false, error);
    return;
  }
  settled.$call(undefined, true, result);
}
