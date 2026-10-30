// The loop of a Bun.go pool thread (BunGo.cpp): it stands in for the thread's entry point.
//
// One goroutine is taken per turn of the event loop. A goroutine runs until it first awaits; then
// the thread takes the next. A thread that finds nothing to take is idle until BunGo.cpp wakes it.

const { take, settle, compile } = $cpp("BunGo.cpp", "createGoRuntimeBinding") as {
  take(): Task | Failed | undefined;
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

function errorRecord(error: Error, depth: number, withProps: boolean) {
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
    props: object | undefined;
  } = { name, message, stack, props: undefined };
  if (withProps) {
    if ("cause" in error && depth < kMaxCauseDepth) record.cause = thrown(error.cause, depth + 1, true);
    const props: Record<string, unknown> = { ...error };
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

function thrown(value: unknown, depth: number, withProps: boolean) {
  return value instanceof Error ? ["error", errorRecord(value, depth, withProps)] : ["value", value];
}

function fail(id: number, error: unknown) {
  try {
    settle(id, 1, thrown(error, 0, true));
    return;
  } catch {}
  // Something it carries can't be cloned: its name, message and stack still can.
  try {
    settle(id, 1, thrown(error instanceof Error ? error : new Error(String(error)), 0, false));
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

async function start(task: Task) {
  const id = task[0];
  let value;
  try {
    const fn =
      task[2] === 0 ? await exported(task[3], task[4], task[5]) : inline(task[4], task[3], task[5], task[6], task[7]);
    value = await fn.$apply(undefined, task[1]);
  } catch (error) {
    fail(id, error);
    return;
  }
  try {
    settle(id, 0, value);
  } catch (error) {
    // The result can't be cloned.
    fail(id, error);
  }
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

pull();

export default {
  pull,
};
