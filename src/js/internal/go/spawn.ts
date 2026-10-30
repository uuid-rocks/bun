// Bun.go: start a goroutine on the pool (BunGo.cpp), on any JS thread.

const { spawn, keepAlive, moduleOf, resolveModule } = $cpp("BunGo.cpp", "createGoSpawnBinding") as {
  spawn(module: string, exportName: string, fromCommonJSExports: boolean, args: unknown[]): number;
  keepAlive(delta: number): void;
  moduleOf(fn: Function): [url: string, namespace: object | undefined] | undefined;
  resolveModule(specifier: string): string;
};

// [module key, export name, the export is a property of a CommonJS module's `module.exports`]
type Target = [module: string, exportName: string, fromCommonJSExports: boolean];

type Pending = {
  resolve: (value: unknown) => void;
  reject: (reason: unknown) => void;
  keepsAlive: boolean;
};

const pending = new Map<number, Pending>();
const targets = new WeakMap<Function, Target>();

class Goroutine<T> extends Promise<T> {
  #id = 0;

  static get [Symbol.species]() {
    return Promise;
  }

  static start<T>(id: number): Goroutine<T> {
    let entry!: Pending;
    const goroutine = new Goroutine<T>((resolve, reject) => {
      entry = { resolve, reject, keepsAlive: true };
    });
    goroutine.#id = id;
    pending.$set(id, entry);
    keepAlive(1);
    return goroutine;
  }

  ref(): this {
    const entry = pending.$get(this.#id);
    if (entry !== undefined && !entry.keepsAlive) {
      entry.keepsAlive = true;
      keepAlive(1);
    }
    return this;
  }

  unref(): this {
    const entry = pending.$get(this.#id);
    if (entry !== undefined && entry.keepsAlive) {
      entry.keepsAlive = false;
      keepAlive(-1);
    }
    return this;
  }
}

function keyOf(object: object, fn: Function): string | undefined {
  // Reflect.ownKeys reads no binding: a namespace throws for an export still in its TDZ.
  for (const key of Reflect.ownKeys(object)) {
    if (typeof key !== "string") continue;
    let value;
    try {
      value = object[key];
    } catch {
      continue;
    }
    if (value === fn) return key;
  }
}

function notSendable(fn: Function, url: string | undefined) {
  const label = fn.name ? `"${fn.name}"` : "This function";
  if (url === undefined) {
    return $ERR_GOROUTINE_NOT_SENDABLE(
      `${label} has no module of its own (it is native or bound), so Bun.go() can't load it on another thread. Pass a function that a module exports.`,
    );
  }
  return $ERR_GOROUTINE_NOT_SENDABLE(
    `${label} is not exported by ${url}, so Bun.go() can't load it on another thread (a closure's variables can't move between threads). Export it from its module and pass data as arguments, or pass the module's URL to run its default export: Bun.go(new URL("./task.ts", import.meta.url)).`,
  );
}

// The exported function of a module: its module's key and the name it is exported under.
function exportOf(fn: Function): Target {
  let target = targets.get(fn);
  if (target !== undefined) return target;
  const source = moduleOf(fn);
  if (source !== undefined) {
    const [url, namespace] = source;
    if (namespace !== undefined) {
      const key = keyOf(namespace, fn);
      if (key !== undefined) target = [url, key, false];
    }
    if (target === undefined) {
      const module = $requireMap.$get(url);
      if (module !== undefined) {
        const exports = module.exports;
        if (exports === fn) {
          target = [url, "default", false];
        } else if ($isObject(exports) || $isCallable(exports)) {
          const key = keyOf(exports, fn);
          if (key !== undefined) target = [url, key, true];
        }
      }
    }
  }
  if (target === undefined) throw notSendable(fn, source?.[0]);
  targets.set(fn, target);
  return target;
}

function go(task: unknown, ...args: unknown[]) {
  let target: Target;
  if ($isCallable(task)) {
    target = exportOf(task as Function);
  } else if (typeof task === "string") {
    target = [resolveModule(task.startsWith("file:") ? Bun.fileURLToPath(task) : task), "default", false];
  } else if (task instanceof URL) {
    target = [resolveModule(task.protocol === "file:" ? Bun.fileURLToPath(task) : task.href), "default", false];
  } else {
    throw $ERR_INVALID_ARG_TYPE("fn", ["function", "string", "URL"], task);
  }
  return Goroutine.start(spawn(target[0], target[1], target[2], args));
}

const errorConstructors = {
  __proto__: null,
  Error,
  TypeError,
  RangeError,
  SyntaxError,
  ReferenceError,
  EvalError,
  URIError,
};

type ErrorRecord = {
  name: string;
  message: string;
  stack: string | undefined;
  cause?: Thrown;
  props: object | undefined;
};
// What a goroutine threw (internal/go/runtime): an Error as its parts, which the structured clone
// would not all keep, or another value as it was.
type Thrown = ["error", ErrorRecord] | ["value", unknown];

function fromThread(thrown: Thrown): unknown {
  if (thrown[0] !== "error") return thrown[1];
  const { name, message, stack, props } = thrown[1];
  const Constructor = errorConstructors[name] ?? Error;
  const error =
    "cause" in thrown[1] ? new Constructor(message, { cause: fromThread(thrown[1].cause!) }) : new Constructor(message);
  if (error.name !== name) {
    Object.defineProperty(error, "name", { value: name, writable: true, configurable: true, enumerable: false });
  }
  if (stack !== undefined) {
    Object.defineProperty(error, "stack", { value: stack, writable: true, configurable: true, enumerable: false });
  }
  if (props !== undefined) Object.assign(error, props);
  return error;
}

const enum Outcome {
  Fulfilled = 0,
  // A value from the goroutine's thread (what it threw)
  Rejected = 1,
  // An error raised on this thread (the goroutine's thread ended, the result could not be read)
  Failed = 2,
}

// BunGo.cpp, on this thread: goroutine `id` has settled.
function settle(id: number, outcome: Outcome, value: unknown) {
  const entry = pending.$get(id);
  if (entry === undefined) return;
  pending.$delete(id);
  if (entry.keepsAlive) keepAlive(-1);
  if (outcome === Outcome.Fulfilled) entry.resolve(value);
  else entry.reject(outcome === Outcome.Rejected ? fromThread(value as Thrown) : value);
}

export default {
  go,
  settle,
};
