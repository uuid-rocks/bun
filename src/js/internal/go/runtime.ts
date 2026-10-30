// The loop of a Bun.go pool thread (BunGo.cpp): it stands in for the thread's entry point.
//
// One goroutine is taken per turn of the event loop. A goroutine runs until it first awaits; then
// the thread takes the next. A thread that finds nothing to take is idle until BunGo.cpp wakes it.

const { take, settle, compile, describe, run, job } = $cpp("BunGo.cpp", "createGoRuntimeBinding") as {
  take(): Task | Failed | undefined;
  // job(fn, args, settled) on a microtask with no frame of this module under it: fn(...args), then
  // settled(true, result) or settled(false, what it threw).
  run(job: Function, fn: Function, args: unknown[], settled: (ok: boolean, value: unknown) => void): void;
  job: Function;
  describe(value: unknown): [path: string, what: string] | undefined;
  settle(id: number, outcome: 0 | 1, value: unknown): void;
  compile(
    source: string,
    module: string,
    line: number,
    column: number,
    strict: boolean,
  ): (require: unknown) => Function;
};

// internal/go/spawn's targets.
type Task =
  | [id: number, args: unknown[], kind: 0, module: string, exportName: string, fromCommonJSExports: boolean]
  | [
      id: number,
      args: unknown[],
      kind: 1,
      module: string,
      source: string,
      line: number,
      column: number,
      strict: boolean,
    ];
// The goroutine's arguments could not be read on this thread.
type Failed = [id: number, error: unknown];

// The `require` an inline function written in `module` gets.
function requireOf(module: string) {
  const { createRequire } = require("node:module");
  try {
    return createRequire(module);
  } catch {
    // A module with no path, such as `bun -e`'s
    return createRequire(require("node:path").join(process.cwd(), "[eval]"));
  }
}

// Inline functions compiled on this thread, by source.
const compiled = new Map<string, { module: string; line: number; column: number; fn: Function }>();
const kMaxCompiled = 1024;

const kMaxCauseDepth = 8;

// Encoded apart from the other own properties of an error.
const kEncoded = new Set(["name", "message", "stack", "cause"]);

function errorRecord(error: Error, depth: number, withProps: boolean, args: unknown[]) {
  let name, message, stack;
  try {
    name = String(error.name);
  } catch {
    name = "Error";
  }
  try {
    message = String(error.message);
  } catch {
    message = "";
  }
  try {
    stack = typeof error.stack === "string" ? error.stack : undefined;
  } catch {}
  const record: {
    name: string;
    message: string;
    stack: string | undefined;
    cause?: unknown;
    errors?: unknown[];
    domException?: true;
    props: object | undefined;
  } = { name, message, stack, props: undefined };
  if (error instanceof DOMException) record.domException = true;
  if (withProps) {
    if ("cause" in error && depth < kMaxCauseDepth) record.cause = thrown(error.cause, depth + 1, true, args);
    let errors;
    if (error instanceof AggregateError && depth < kMaxCauseDepth) {
      try {
        errors = error.errors;
      } catch {}
    }
    if ($isArray(errors)) record.errors = errors.map(inner => thrown(inner, depth + 1, true, args));
    const props: Record<string, unknown> = { ...error };
    for (const key of kEncoded) delete props[key];
    if (record.errors !== undefined) delete props.errors;
    // Node's error codes are not enumerable.
    let code;
    try {
      code = (error as { code?: unknown }).code;
    } catch {}
    if (typeof code === "string" && !("code" in props)) props.code = code;
    if (Reflect.ownKeys(props).length > 0) record.props = props;
  }
  return record;
}

// The reason of an AbortSignal among the arguments is sent as which argument it is: the goroutine rejects with
// the original reason, which its caller can tell apart from another error by identity.
function thrown(value: unknown, depth: number, withProps: boolean, args: unknown[]) {
  if ($isObject(value)) {
    for (let i = 0; i < args.length; i++) {
      const arg = args[i];
      if (arg instanceof AbortSignal && arg.aborted && arg.reason === value) return ["signal", i];
    }
  }
  return value instanceof Error ? ["error", errorRecord(value, depth, withProps, args)] : ["value", value];
}

function fail(id: number, error: unknown, args: unknown[] = []) {
  try {
    settle(id, 1, thrown(error, 0, true, args));
    return;
  } catch {}
  // Something it carries can't be cloned: its name, message and stack still can.
  try {
    settle(id, 1, thrown(error instanceof Error ? error : new Error(String(error)), 0, false, args));
    return;
  } catch {}
  settle(id, 1, [
    "error",
    { name: "Error", message: "The goroutine threw a value that can't be cloned", stack: undefined, props: undefined },
  ]);
}

async function exported(module: string, exportName: string, fromCommonJSExports: boolean) {
  const namespace = await import(module);
  let fn = fromCommonJSExports ? namespace.default?.[exportName] : namespace[exportName];
  // A bundle names the export of a CommonJS module by a URL fragment, and that module's entry chunk exports
  // only `module.exports`, as `default`.
  if (fn === undefined && !fromCommonJSExports && exportName !== "default" && !(exportName in namespace)) {
    fn = namespace.default?.[exportName];
  }
  if (!$isCallable(fn)) {
    throw $ERR_GOROUTINE_NOT_SENDABLE(
      exportName === "default"
        ? `The default export of ${module} is not a function`
        : `The export "${exportName}" of ${module} is not a function`,
    );
  }
  return fn;
}

function inline(source: string, module: string, line: number, column: number, strict: boolean) {
  const entry = compiled.$get(source);
  if (entry !== undefined && entry.module === module && entry.line === line && entry.column === column) {
    return entry.fn;
  }
  if (compiled.size >= kMaxCompiled) compiled.clear();
  const fn = compile(source, module, line, column, strict)(requireOf(module));
  compiled.$set(source, { module, line, column, fn });
  return fn;
}

// What in the result can't be cloned.
function resultCloneError(error: unknown, value: unknown) {
  if ((error as Error)?.name !== "DataCloneError") return error;
  const found = describe(value);
  if (found === undefined) return error;
  const [path, what] = found;
  return new DOMException(
    path === ""
      ? `Bun.go(): the goroutine returned ${what}, which can't be sent to another thread.`
      : `Bun.go(): the goroutine returned ${what} at ${path}, which can't be sent to another thread.`,
    "DataCloneError",
  );
}

async function start(task: Task) {
  const id = task[0];
  let fn;
  try {
    fn =
      task[2] === 0 ? await exported(task[3], task[4], task[5]) : inline(task[4], task[3], task[5], task[6], task[7]);
  } catch (error) {
    fail(id, error);
    return;
  }
  const args = task[1];
  run(job, fn, args, (ok, value) => {
    if (!ok) {
      fail(id, value, args);
      return;
    }
    // Not awaited by an async function of this module: its frames stay out of the goroutine's.
    Promise.$resolve(value).then(
      value => {
        try {
          settle(id, 0, value);
        } catch (error) {
          fail(id, resultCloneError(error, value));
        }
      },
      error => fail(id, error, args),
    );
  });
}

// Also called by BunGo.cpp when this thread was idle and something is queued.
function pull() {
  const task = take();
  if (task === undefined) return;
  // The next is taken on the next turn: the goroutines started meanwhile run until they await.
  setImmediate(pull);
  if (task.length === 2) {
    fail(task[0], task[1]);
    return;
  }
  start(task);
}

require("internal/go/spawn").enterPool();
pull();

export default {
  pull,
};
