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
  const lock = { unlock, [Symbol.dispose]: unlock };
  // What a DataCloneError says to do instead (ChannelMessage.cpp).
  $putByIdDirectPrivate(
    lock,
    "isUncloneable",
    "a lock belongs to the goroutine that took it: pass the Mutex and lock it there",
  );
  return lock;
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
  const lock = { unlock, [Symbol.dispose]: unlock };
  // What a DataCloneError says to do instead (ChannelMessage.cpp).
  $putByIdDirectPrivate(
    lock,
    "isUncloneable",
    "a lock belongs to the goroutine that took it: pass the Mutex and lock it there",
  );
  return lock;
}
