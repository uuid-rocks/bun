// Bun.go: start a goroutine on the pool (BunGo.cpp), on any JS thread.

const {
  spawn,
  spawnInline,
  keepAlive,
  moduleOf,
  resolveModule,
  sourceOf,
  check,
  codeGenerationAllowed,
  evaluating,
  describe,
  hasModuleRequire,
} = $cpp("BunGo.cpp", "createGoSpawnBinding") as {
  spawn(module: string, exportName: string, fromCommonJSExports: boolean, args: unknown[]): number;
  spawnInline(source: string, module: string, line: number, column: number, strict: boolean, args: unknown[]): number;
  keepAlive(delta: number): void;
  moduleOf(fn: Function): [url: string, namespace: object | undefined, bundled: boolean] | undefined;
  resolveModule(specifier: string): string;
  sourceOf(fn: Function): Source | undefined;
  check(fn: Function, source: string, module: string, line: number, column: number): Problem[] | undefined;
  codeGenerationAllowed(): boolean;
  evaluating(module: string): boolean;
  describe(value: unknown): [path: string, what: string] | undefined;
  hasModuleRequire(fn: Function, module: string): boolean;
};

// A function's text, and where it starts in its module (zero-based).
type Source = [source: string, module: string, line: number, column: number, strict: boolean];
type ProblemKind =
  "unparsable" | "class" | "this" | "arguments" | "import.meta" | "eval" | "captured" | "imported" | "undeclared";
// The line is one-based, in the module's own text. What a captured or imported binding holds, and
// where an import comes from (its specifier, and its name there; none for a namespace).
type Problem = [
  kind: ProblemKind,
  name: string,
  line: number,
  valueKind: "function" | "class" | "namespace" | "data" | "bun" | "",
  from: string | undefined,
  importName: string | undefined,
];

const enum TargetKind {
  // [module key, export name, the export is a property of a CommonJS module's `module.exports`]
  Export = 0,
  // A function no module exports, sent as its source.
  Inline = 1,
}
type Target =
  | [TargetKind.Export, module: string, exportName: string, fromCommonJSExports: boolean]
  // `usesRequire`: the text names `require`, which must be its module's own each time it is sent.
  | [TargetKind.Inline, ...Source, usesRequire: boolean];

type Pending = {
  resolve: (value: unknown) => void;
  reject: (reason: unknown) => void;
  keepsAlive: boolean;
};

const pending = new Map<number, Pending>();
// internal/go/runtime runs here: modules are imported here to run goroutines.
let onPoolThread = false;
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

// bun build finds the module of a function only from the call itself.
const inBundle =
  ' In a bundle, pass Bun.go() the imported binding itself, as in Bun.go(task) or Bun.go(tasks.task), or the module\'s URL with the export as its fragment, as in Bun.go(new URL("./tasks.ts#task", import.meta.url)): bun build makes that module an entry point. List a module named any other way as an entry point of the build.';

function notSendable(fn: Function, url: string | undefined, problems?: Problem[], bundled = false) {
  const error = notSendableIn(fn, url, problems);
  if (bundled) error.message += inBundle;
  return error;
}

function notSendableIn(fn: Function, url: string | undefined, problems?: Problem[]) {
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
  const reasons: string[] = [];
  for (const [kind, name, line, valueKind, from, importName] of problems) {
    const uses = `it uses "${name}" (${at(line)})`;
    switch (kind) {
      case "captured":
        if (valueKind === "bun") {
          reasons.push(`${uses}, which is Bun.${name}: use Bun.${name} instead`);
        } else if (valueKind === "function" || valueKind === "class") {
          reasons.push(
            `${uses}, a ${valueKind} of the code around it: define it inside this function, or export it from a module other than the entry point and import it inside this function with \`await import()\``,
          );
        } else if (valueKind === "namespace") {
          reasons.push(`${uses}, a module's namespace: import the module inside this function with \`await import()\``);
        } else if (valueKind === "data") {
          reasons.push(`${uses}, a variable of the code around it: pass it as an argument`);
        } else {
          // Not readable yet, as a function declaration may not be.
          reasons.push(
            `${uses}, which belongs to the code around it: pass it as an argument, or, for a function, define it inside this function or export it from a module other than the entry point and import it inside this function with \`await import()\``,
          );
        }
        break;
      case "imported": {
        const imports = `${uses}, which its module imports from "${from}"`;
        if (from === "bun" || valueKind === "bun") {
          reasons.push(
            `${imports}: use ${importName === undefined || importName === "default" ? "the global Bun" : `Bun.${importName}`} instead`,
          );
        } else if (valueKind === "function") {
          reasons.push(
            `${imports}: pass it to Bun.go() itself, as in Bun.go(${name}, ...args), or import it inside this function with \`await import(${JSON.stringify(from)})\``,
          );
        } else {
          reasons.push(
            `${imports}: import it inside this function with \`await import(${JSON.stringify(from)})\`, or pass it as an argument`,
          );
        }
        break;
      }
      case "undeclared":
        reasons.push(`${uses}, which is neither its own nor a global: define it inside this function`);
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
  return $ERR_GOROUTINE_NOT_SENDABLE(`Bun.go() can't run ${label} on another thread: ${reasons.join("; ")}.`);
}

// The function a module exports: its module's key and the name it is exported under.
function exportOf(fn: Function, source: ReturnType<typeof moduleOf>): Target | undefined {
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
function inlineOf(fn: Function, source: Source | undefined, bundled: boolean): Target {
  if (source === undefined) throw notSendable(fn, undefined);
  const [text, module, line, column, strict] = source;
  let verdict = verdicts.$get(text);
  if (verdict === undefined || verdict.module !== module || verdict.line !== line || verdict.column !== column) {
    if (verdicts.size >= kMaxVerdicts) verdicts.clear();
    const problems = check(fn, text, module, line, column);
    verdict = {
      module,
      line,
      column,
      target:
        problems === undefined
          ? [TargetKind.Inline, text, module, line, column, strict, /\brequire\b/.test(text)]
          : undefined,
      problems,
    };
    verdicts.$set(text, verdict);
  }
  const { problems } = verdict;
  if (problems !== undefined) throw notSendable(fn, module, problems, bundled);
  if (!codeGenerationAllowed()) throw notSendable(fn, module, undefined, bundled);
  return verdict.target!;
}

function targetOf(fn: Function): Target {
  const target = cachedTargetOf(fn);
  // What `require` is depends on the closure, and a variable can be assigned another: the verdict
  // of the first function made by this code says nothing about it.
  if (target[0] === TargetKind.Inline && target[6] && !hasModuleRequire(fn, target[2])) {
    const problems = check(fn, target[1], target[2], target[3], target[4]);
    if (problems !== undefined) throw notSendable(fn, target[2], problems);
  }
  return target;
}

function cachedTargetOf(fn: Function): Target {
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
  const module = moduleOf(fn);
  target = exportOf(fn, module) ?? inlineOf(fn, source, module?.[2] ?? false);
  targets.set(fn, target);
  return target;
}

// A URL's fragment names the export to run; `bun build` writes `Bun.go(fn)` as one.
function urlTarget(url: URL): Target {
  let exportName = "default";
  if (url.hash.length > 1) {
    exportName = decodeURIComponent(url.hash.slice(1));
    url = new URL(url.href);
    url.hash = "";
  }
  return [
    TargetKind.Export,
    resolveModule(url.protocol === "file:" ? Bun.fileURLToPath(url) : url.href),
    exportName,
    false,
  ];
}

function go(task: unknown, ...args: unknown[]) {
  let target: Target;
  if ($isCallable(task)) {
    target = targetOf(task as Function);
  } else if (typeof task === "string") {
    target = task.startsWith("file:")
      ? urlTarget(new URL(task))
      : [TargetKind.Export, resolveModule(task), "default", false];
  } else if (task instanceof URL) {
    target = urlTarget(task);
  } else {
    throw $ERR_INVALID_ARG_TYPE("fn", ["function", "string", "URL"], task);
  }
  let id: number;
  if (target[0] === TargetKind.Export) {
    if (onPoolThread && evaluating(target[1])) throw evaluatingHere(task, target[1], target[2]);
    try {
      id = spawn(target[1], target[2], target[3], args);
    } catch (error) {
      throw argumentCloneError(error, args);
    }
  } else {
    try {
      id = spawnInline(target[1], target[2], target[3], target[4], target[5], args);
    } catch (error) {
      throw argumentCloneError(error, args);
    }
  }
  return Goroutine.start(id);
}

// A pool thread imports the module of each export it runs. A module this thread is still evaluating
// (the entry point's top level, say, waiting on its own goroutines) would wait for the goroutine,
// which would wait for the module.
function evaluatingHere(task: unknown, module: string, exportName: string) {
  const label = $isCallable(task) && (task as Function).name ? `"${(task as Function).name}"` : `"${exportName}"`;
  return $ERR_GOROUTINE_NOT_SENDABLE(
    `Bun.go() can't run ${label} here: it is exported by ${module}, which this pool thread is still evaluating (a pool thread evaluates the module of each export it runs). The goroutine would wait for the module, and the module for the goroutine. Run that module's top-level code only on the main thread, inside if (import.meta.main) { ... }, or move ${label} to a module of its own.`,
  );
}

// What in the arguments can't be cloned, and which argument holds it.
function argumentCloneError(error: unknown, args: unknown[]) {
  if ((error as Error)?.name !== "DataCloneError") return error;
  for (let i = 0; i < args.length; i++) {
    const found = describe(args[i]);
    if (found === undefined) continue;
    const [path, what] = found;
    return new DOMException(
      path === ""
        ? `Bun.go() argument ${i + 1} is ${what}, which can't be sent to another thread.`
        : `Bun.go() argument ${i + 1} has ${what} at ${path}, which can't be sent to another thread.`,
      "DataCloneError",
    );
  }
  return error;
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
  // An AggregateError's
  errors?: Thrown[];
  // A DOMException: its name gives its code.
  domException?: true;
  props: object | undefined;
};
// What a goroutine threw (internal/go/runtime): an Error as its parts, which the structured clone
// would not all keep, or another value as it was.
type Thrown = ["error", ErrorRecord] | ["value", unknown];

function fromThread(thrown: Thrown): unknown {
  if (thrown[0] !== "error") return thrown[1];
  const record = thrown[1];
  const { name, message, stack, props } = record;
  const options = "cause" in record ? { cause: fromThread(record.cause!) } : undefined;
  let error;
  if (record.domException) {
    error = new DOMException(message, name);
    if (options !== undefined) {
      Object.defineProperty(error, "cause", {
        value: options.cause,
        writable: true,
        configurable: true,
        enumerable: false,
      });
    }
  } else if (record.errors !== undefined) {
    error = new AggregateError(record.errors.map(fromThread), message, options);
  } else {
    const Constructor = errorConstructors[name] ?? Error;
    error = new Constructor(message, options);
  }
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
  // internal/go/runtime, when it starts on a pool thread
  enterPool() {
    onPoolThread = true;
  },
};
