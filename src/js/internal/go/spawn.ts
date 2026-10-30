// Bun.go: start a goroutine on the pool (BunGo.cpp), on any JS thread.

const { spawn, spawnInline, keepAlive, moduleOf, resolveModule, sourceOf, check, codeGenerationAllowed } = $cpp(
  "BunGo.cpp",
  "createGoSpawnBinding",
) as {
  spawn(module: string, exportName: string, fromCommonJSExports: boolean, args: unknown[]): number;
  spawnInline(source: string, module: string, line: number, column: number, strict: boolean, args: unknown[]): number;
  keepAlive(delta: number): void;
  moduleOf(fn: Function): [url: string, namespace: object | undefined] | undefined;
  resolveModule(specifier: string): string;
  sourceOf(fn: Function): Source | undefined;
  check(fn: Function, source: string, module: string, line: number, column: number): Checked;
  codeGenerationAllowed(): boolean;
};

// A function's text, and where it starts in its module (zero-based).
type Source = [source: string, module: string, line: number, column: number, strict: boolean];
type ProblemKind =
  "unparsable" | "class" | "this" | "arguments" | "import.meta" | "eval" | "captured" | "imported" | "undeclared";
// The line is one-based, in the module's own text.
type Problem = [kind: ProblemKind, name: string, line: number];
// Where the function starts in its module's own text (zero-based), which Bun may have transpiled,
// and what keeps it from running on another thread.
type Checked = [line: number, column: number, problems?: Problem[]];

const enum TargetKind {
  // [module key, export name, the export is a property of a CommonJS module's `module.exports`]
  Export = 0,
  // A function no module exports, sent as its source.
  Inline = 1,
}
type Target =
  | [TargetKind.Export, module: string, exportName: string, fromCommonJSExports: boolean]
  | [TargetKind.Inline, ...Source];

type Pending = {
  resolve: (value: unknown) => void;
  reject: (reason: unknown) => void;
  keepsAlive: boolean;
};

const pending = new Map<number, Pending>();
const targets = new WeakMap<Function, Target>();
// What `check` found for the code of a function no module exports, by its source: an inline
// function is a new object each time the code around it runs.
type Verdict = { module: string; line: number; column: number; target: Target | undefined; problems?: Problem[] };
const verdicts = new Map<string, Verdict>();
const kMaxVerdicts = 1024;

class Goroutine<T> extends Promise<T> {
  #id = 0;

  static get [Symbol.species]() {
    return Promise;
  }

  static start<T>(id: number): Goroutine<T> {
    let entry!: Pending;
    const goroutine = new Goroutine<T>((resolve, reject) => {
      entry = { resolve: resolve as Pending["resolve"], reject, keepsAlive: true };
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

function list(items: string[]) {
  return items.length === 1 ? items[0] : `${items.slice(0, -1).join(", ")} and ${items[items.length - 1]}`;
}

function notSendable(fn: Function, url: string | undefined, problems?: Problem[]) {
  const label = fn.name ? `"${fn.name}"` : "this function";
  if (url === undefined) {
    return $ERR_GOROUTINE_NOT_SENDABLE(
      `Bun.go() can't run ${label} on another thread: it has no source of its own (it is native or bound). Pass a function written in your code.`,
    );
  }
  if (problems === undefined) {
    return $ERR_GOROUTINE_NOT_SENDABLE(
      `Bun.go() can't run ${label} on another thread: no module exports it, and its source can't be compiled again there while code generation from strings is disallowed. Export it from its module.`,
    );
  }
  const [kind] = problems[0];
  if (kind === "unparsable" || kind === "class") {
    return $ERR_GOROUTINE_NOT_SENDABLE(
      `Bun.go() can't run ${label} on another thread: it is ${kind === "class" ? "a class" : "a method"}. Pass a function or an arrow function.`,
    );
  }
  const at = (line: number) => `${url}:${line}`;
  const outside: string[] = [];
  const reasons: string[] = [];
  for (const [kind, name, line] of problems) {
    switch (kind) {
      case "captured":
      case "imported":
      case "undeclared":
        outside.push(`"${name}" (${at(line)})`);
        break;
      case "this":
        reasons.push(
          `the arrow function uses the \`this\` of the code around it (${at(line)}): pass it as an argument`,
        );
        break;
      case "arguments":
        reasons.push(
          `the arrow function uses the \`arguments\` of the function around it (${at(line)}): pass them as arguments`,
        );
        break;
      case "import.meta":
        reasons.push(`it uses \`import.meta\` (${at(line)}): pass what it needs from it as an argument`);
        break;
      case "eval":
        reasons.push(
          `it calls \`eval()\` directly (${at(line)}), which can read the variables around it: call \`(0, eval)()\` instead`,
        );
        break;
    }
  }
  const { length } = outside;
  if (length > 0) {
    const one = length === 1;
    reasons.unshift(
      `it uses ${list(outside)}, which ${one ? "is neither its own nor a global" : "are neither its own nor globals"}. ${one ? "Pass it as an argument" : "Pass them as arguments"}, or import ${one ? "it" : "them"} inside the function with \`await import()\``,
    );
  }
  return $ERR_GOROUTINE_NOT_SENDABLE(`Bun.go() can't run ${label} on another thread: ${reasons.join("; ")}.`);
}

// The function a module exports: its module's key and the name it is exported under.
function exportOf(fn: Function): Target | undefined {
  const source = moduleOf(fn);
  if (source === undefined) return;
  const [url, namespace] = source;
  if (namespace !== undefined) {
    const key = keyOf(namespace, fn);
    if (key !== undefined) return [TargetKind.Export, url, key, false];
  }
  const module = $requireMap.$get(url);
  if (module === undefined) return;
  const exports = module.exports;
  if (exports === fn) return [TargetKind.Export, url, "default", false];
  if ($isObject(exports) || $isCallable(exports)) {
    const key = keyOf(exports, fn);
    if (key !== undefined) return [TargetKind.Export, url, key, true];
  }
}

// A function that uses nothing but its parameters, what it declares, and globals: its source.
function inlineOf(fn: Function, source: Source | undefined): Target {
  if (source === undefined) throw notSendable(fn, undefined);
  const [text, module, line, column, strict] = source;
  let verdict = verdicts.$get(text);
  if (verdict === undefined || verdict.module !== module || verdict.line !== line || verdict.column !== column) {
    if (verdicts.size >= kMaxVerdicts) verdicts.clear();
    const [originalLine, originalColumn, problems] = check(fn, text, module, line, column);
    verdict = {
      module,
      line,
      column,
      target:
        problems === undefined ? [TargetKind.Inline, text, module, originalLine, originalColumn, strict] : undefined,
      problems,
    };
    verdicts.$set(text, verdict);
  }
  const { problems } = verdict;
  if (problems !== undefined) throw notSendable(fn, module, problems);
  if (!codeGenerationAllowed()) throw notSendable(fn, module);
  return verdict.target!;
}

function targetOf(fn: Function): Target {
  let target = targets.get(fn);
  if (target !== undefined) return target;
  const source = sourceOf(fn);
  if (source !== undefined) {
    // Code that made a function no module exports makes another: no need to look for it in the
    // module's exports.
    const verdict = verdicts.$get(source[0]);
    if (
      verdict?.target !== undefined &&
      verdict.module === source[1] &&
      verdict.line === source[2] &&
      verdict.column === source[3]
    ) {
      return verdict.target;
    }
  }
  target = exportOf(fn) ?? inlineOf(fn, source);
  targets.set(fn, target);
  return target;
}

function go(task: unknown, ...args: unknown[]) {
  let target: Target;
  if ($isCallable(task)) {
    target = targetOf(task as Function);
  } else if (typeof task === "string") {
    target = [
      TargetKind.Export,
      resolveModule(task.startsWith("file:") ? Bun.fileURLToPath(task) : task),
      "default",
      false,
    ];
  } else if (task instanceof URL) {
    target = [
      TargetKind.Export,
      resolveModule(task.protocol === "file:" ? Bun.fileURLToPath(task) : task.href),
      "default",
      false,
    ];
  } else {
    throw $ERR_INVALID_ARG_TYPE("fn", ["function", "string", "URL"], task);
  }
  if (target[0] === TargetKind.Export) return Goroutine.start(spawn(target[1], target[2], target[3], args));
  return Goroutine.start(spawnInline(target[1], target[2], target[3], target[4], target[5], args));
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
