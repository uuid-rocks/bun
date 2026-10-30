// The loop of a Bun.go pool thread (BunGo.cpp): it stands in for the thread's entry point.
//
// One goroutine is taken per turn of the event loop. A goroutine runs until it first awaits; then
// the thread takes the next. A thread that finds nothing to take is idle until BunGo.cpp hands it
// a goroutine through `run`.

const { take, settle } = $cpp("BunGo.cpp", "createGoRuntimeBinding") as {
  take(): Task | Failed | undefined;
  settle(id: number, outcome: 0 | 1, value: unknown): void;
};

type Task = [id: number, module: string, exportName: string, fromCommonJSExports: boolean, args: unknown[]];
// The goroutine's arguments could not be read on this thread.
type Failed = [id: number, error: unknown];

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
    const props = { ...error };
    if ($isObject(props) && Reflect.ownKeys(props).length > 0) record.props = props;
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

async function start(id: number, module: string, exportName: string, fromCommonJSExports: boolean, args: unknown[]) {
  let value;
  try {
    const namespace = await import(module);
    const fn = fromCommonJSExports ? namespace.default?.[exportName] : namespace[exportName];
    if (!$isCallable(fn)) {
      throw $ERR_GOROUTINE_NOT_SENDABLE(
        exportName === "default"
          ? `The default export of ${module} is not a function`
          : `The export "${exportName}" of ${module} is not a function`,
      );
    }
    value = await fn.$apply(undefined, args);
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

function pull() {
  const task = take();
  if (task !== undefined) run(task);
}

function run(task: Task | Failed) {
  setImmediate(pull);
  if (task.length === 2) {
    fail(task[0], task[1]);
    return;
  }
  start(task[0], task[1], task[2], task[3], task[4]);
}

pull();

export default {
  run,
  fail,
};
