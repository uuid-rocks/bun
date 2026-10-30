type Mutex = import("bun").Mutex;

// What unlocks the mutex once: `unlock()` or the end of a `using` scope, whichever comes first.
export async function lock(this: Mutex, options?: { signal?: AbortSignal }) {
  const mutex = this;
  await mutex.$mutexLock(options);
  let held = true;
  const unlock = () => {
    if (!held) return;
    held = false;
    mutex.$mutexUnlock();
  };
  return { unlock, [Symbol.dispose]: unlock };
}

export function tryLock(this: Mutex) {
  const mutex = this;
  if (!mutex.$mutexTryLock()) return undefined;
  let held = true;
  const unlock = () => {
    if (!held) return;
    held = false;
    mutex.$mutexUnlock();
  };
  return { unlock, [Symbol.dispose]: unlock };
}
