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
  readsMain,
  ownedByThisThread,
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
  describe(value: unknown): [path: string, what: string, hint: string | undefined] | undefined;
  hasModuleRequire(fn: Function, module: string): boolean;
  readsMain(fn: Function): boolean;
  // A structured clone of `object` throws a DataCloneError that says `hint`.
  ownedByThisThread(object: object, hint: string): void;
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
  valueKind: "function" | "native" | "class" | "namespace" | "data" | "bun" | "",
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
  // The AbortSignals among the arguments, by index: a goroutine that throws its copy of one's reason rejects
  // with the reason itself.
  signals: AbortSignal[] | undefined;
};

const pending = new Map<number, Pending>();
// internal/go/runtime runs here: modules are imported here to run goroutines.
let onPoolThread = false;
const targets = new WeakMap<Function, Target>();
// What `bun build` found a function passed to a `.go()` call to be: its module's entry chunk, by URL.
const bundledTasks = new WeakMap<Function, URL>();
// What `check` found for the code of a function no module exports, by its source: an inline
// function is a new object each time the code around it runs.
type Verdict = { module: string; line: number; column: number; target: Target | undefined; problems?: Problem[] };
const verdicts = new Map<string, Verdict>();
const kMaxVerdicts = 1024;

const kGoroutineHint = "a goroutine belongs to the thread that started it: await it, and send its result";

class Goroutine<T> extends Promise<T> {
  #id = 0;
  // Until it starts (a WaitGroup's limit can hold it back)
  #keepsAlive = true;
  #resolve!: Pending["resolve"];
  #reject!: Pending["reject"];

  static get [Symbol.species]() {
    return Promise;
  }

  static create<T>(): Goroutine<T> {
    let resolve!: Pending["resolve"], reject!: Pending["reject"];
    const goroutine = new Goroutine<T>((res, rej) => {
      resolve = res as Pending["resolve"];
      reject = rej;
    });
    goroutine.#resolve = resolve;
    goroutine.#reject = reject;
    ownedByThisThread(goroutine, kGoroutineHint);
    return goroutine;
  }

  static start(goroutine: Goroutine<unknown>, id: number, signals: AbortSignal[] | undefined) {
    goroutine.#id = id;
    const keepsAlive = goroutine.#keepsAlive;
    pending.$set(id, { resolve: goroutine.#resolve, reject: goroutine.#reject, keepsAlive, signals });
    if (keepsAlive) keepAlive(1);
  }

  // It never started.
  static fail(goroutine: Goroutine<unknown>, error: unknown) {
    goroutine.#reject(error);
  }

  ref(): this {
    if (this.#id === 0) {
      this.#keepsAlive = true;
      return this;
    }
    const entry = pending.$get(this.#id);
    if (entry !== undefined && !entry.keepsAlive) {
      entry.keepsAlive = true;
      keepAlive(1);
    }
    return this;
  }

  unref(): this {
    if (this.#id === 0) {
      this.#keepsAlive = false;
      return this;
    }
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
function inBundle(api: string) {
  return ` In a bundle, bun build finds the module of a function passed to ${api}() as an imported binding, as in ${api}(task) or ${api}(tasks.task), or as the module's URL with the export as its fragment, as in ${api}(new URL("./tasks.ts#task", import.meta.url)), and makes that module an entry point. Pass it one of those ways, or list the module that exports it as an entry point of the build and build with --splitting, so that the function is that entry point's export.`;
}

function notSendable(api: string, fn: Function, url: string | undefined, problems?: Problem[], bundled = false) {
  const error = notSendableIn(api, fn, url, problems);
  if (bundled) error.message += inBundle(api);
  return error;
}

function notSendableIn(api: string, fn: Function, url: string | undefined, problems?: Problem[]) {
  const name = fn.name;
  if (url === undefined) {
    // What Function.prototype.bind() makes is named "bound " and the name of what it binds.
    if (typeof name === "string" && name.startsWith("bound ")) {
      return $ERR_GOROUTINE_NOT_SENDABLE(
        `${api}() can't run a bound function on another thread: it has no source of its own. Pass the function itself, and what it was bound to as arguments.`,
      );
    }
    return $ERR_GOROUTINE_NOT_SENDABLE(
      `${api}() can't run ${name ? `"${name}"` : "this function"} on another thread: it has no source of its own (it is native). Pass a function written in your code.`,
    );
  }
  const label = name ? `"${name}"` : "this function";
  if (problems === undefined) {
    return $ERR_GOROUTINE_NOT_SENDABLE(
      `${api}() can't run ${label} on another thread: no module exports it, and its source can't be compiled again there while code generation from strings is disallowed. Export it from its module.`,
    );
  }
  const [kind] = problems[0];
  if (kind === "unparsable" || kind === "class") {
    return $ERR_GOROUTINE_NOT_SENDABLE(
      `${api}() can't run ${label} on another thread: it is ${kind === "class" ? "a class" : "a method"}. Pass a function or an arrow function.`,
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
        } else if (valueKind === "function" || valueKind === "native" || valueKind === "class") {
          reasons.push(
            `${uses}, a ${valueKind === "class" ? "class" : "function"} of the code around it: define it inside this function, or export it from a module other than the entry point and import it inside this function with \`await import()\``,
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
        const importInside =
          importName === undefined
            ? `\`const ${name} = await import(${JSON.stringify(from)})\``
            : `\`const { ${importName === name ? name : `${importName}: ${name}`} } = await import(${JSON.stringify(from)})\``;
        if (from === "bun" || valueKind === "bun") {
          reasons.push(
            `${imports}: use ${importName === undefined || importName === "default" ? "the global Bun" : `Bun.${importName}`} instead`,
          );
        } else if (valueKind === "function") {
          reasons.push(
            `${imports}: pass it to ${api}() itself, as in ${api}(${name}, ...args), or import it inside this function with ${importInside}`,
          );
        } else if (valueKind === "class" || valueKind === "native") {
          // Neither has a source of its own to send, nor can it be an argument.
          reasons.push(
            `${imports}: import it inside this function with ${importInside}, or export a function that uses it from a module of your own and pass that function to ${api}()`,
          );
        } else {
          reasons.push(`${imports}: import it inside this function with ${importInside}, or pass it as an argument`);
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
  return $ERR_GOROUTINE_NOT_SENDABLE(`${api}() can't run ${label} on another thread: ${reasons.join("; ")}.`);
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
function inlineOf(api: string, fn: Function, source: Source | undefined, bundled: boolean): Target {
  if (source === undefined) throw notSendable(api, fn, undefined);
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
    // Whether `require` is the module's own depends on the closure: another closure of this code may pass.
    if (problems === undefined || !problems.some(problem => problem[1] === "require")) verdicts.$set(text, verdict);
  }
  const { problems } = verdict;
  if (problems !== undefined) throw notSendable(api, fn, module, problems, bundled);
  if (!codeGenerationAllowed()) throw notSendable(api, fn, module, undefined, bundled);
  return verdict.target!;
}

function targetOf(api: string, fn: Function): Target {
  const target = cachedTargetOf(api, fn);
  // What `require` is depends on the closure, and a variable can be assigned another: the verdict
  // of the first function made by this code says nothing about it.
  if (target[0] === TargetKind.Inline && target[6] && !hasModuleRequire(fn, target[2])) {
    const problems = check(fn, target[1], target[2], target[3], target[4]);
    if (problems !== undefined) throw notSendable(api, fn, target[2], problems);
  }
  return target;
}

function cachedTargetOf(api: string, fn: Function): Target {
  let target = targets.get(fn);
  if (target !== undefined) return target;
  const bundled = bundledTasks.get(fn);
  if (bundled !== undefined) {
    target = urlTarget(bundled);
    targets.set(fn, target);
    return target;
  }
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
  target = exportOf(fn, module);
  if (target !== undefined) {
    if (!onPoolThread && target[1] === Bun.main && !readsMain(fn)) throw unguardedMain(api, fn, target[1]);
  } else {
    target = inlineOf(api, fn, source, module?.[2] ?? false);
  }
  targets.set(fn, target);
  return target;
}

// A pool thread imports the module of each export it runs. All of the entry point's top-level code would run
// there again, and its own call to go() would wait for itself.
function unguardedMain(api: string, fn: Function, module: string) {
  const label = fn.name ? `"${fn.name}"` : "this function";
  return $ERR_GOROUTINE_NOT_SENDABLE(
    `${api}() can't run ${label} on another thread: it is exported by the entry point, ${module}, which each pool thread would import to run it, and the entry point's top-level code would run there again. Put that code inside if (import.meta.main) { ... }, which is false on a pool thread, or move ${label} to a module of its own.`,
  );
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

function targetFor(api: string, task: unknown): Target {
  if ($isCallable(task)) return targetOf(api, task as Function);
  if (typeof task === "string") {
    return task.startsWith("file:")
      ? urlTarget(new URL(task))
      : [TargetKind.Export, resolveModule(task), "default", false];
  }
  if (task instanceof URL) return urlTarget(task);
  throw $ERR_INVALID_ARG_TYPE("fn", ["function", "string", "URL"], task);
}

// The AbortSignals among the arguments, by index.
function signalsOf(args: unknown[]) {
  let signals: AbortSignal[] | undefined;
  for (let i = 0; i < args.length; i++) {
    if (args[i] instanceof AbortSignal) (signals ??= [])[i] = args[i] as AbortSignal;
  }
  return signals;
}

// Queues the goroutine on the pool.
function start(api: string, goroutine: Goroutine<unknown>, task: unknown, target: Target, args: unknown[]) {
  let id: number;
  if (target[0] === TargetKind.Export) {
    if (onPoolThread && evaluating(target[1])) throw evaluatingHere(api, task, target[1], target[2]);
    try {
      id = spawn(target[1], target[2], target[3], args);
    } catch (error) {
      throw argumentCloneError(api, error, args);
    }
  } else {
    try {
      id = spawnInline(target[1], target[2], target[3], target[4], target[5], args);
    } catch (error) {
      throw argumentCloneError(api, error, args);
    }
  }
  Goroutine.start(goroutine, id, signalsOf(args));
}

// What go() throws itself points at its caller, not at this module.
function thrownBy(error: unknown, caller: Function) {
  if (error instanceof Error && ((error as { code?: unknown }).code !== undefined || error.name === "DataCloneError")) {
    Error.captureStackTrace(error, caller);
  }
  return error;
}

function go(task: unknown, ...args: unknown[]) {
  const goroutine = Goroutine.create();
  try {
    start("Bun.go", goroutine, task, targetFor("Bun.go", task), args);
  } catch (error) {
    throw thrownBy(error, go);
  }
  return goroutine;
}

// `bun build`: `fn` is the export the URL names. A call to any `.go()` method passes it through this, so a call
// that is not to Bun.go() still gets `fn` itself.
function bundledTask(fn: unknown, url: unknown) {
  if ($isCallable(fn) && url instanceof URL) bundledTasks.set(fn as Function, url);
  return fn;
}
Object.defineProperty(go, Symbol.for("Bun.go.task"), { value: bundledTask });

// A pool thread imports the module of each export it runs. A module this thread is still evaluating
// (the entry point's top level, say, waiting on its own goroutines) would wait for the goroutine,
// which would wait for the module.
function evaluatingHere(api: string, task: unknown, module: string, exportName: string) {
  const label = $isCallable(task) && (task as Function).name ? `"${(task as Function).name}"` : `"${exportName}"`;
  return $ERR_GOROUTINE_NOT_SENDABLE(
    `${api}() can't run ${label} here: it is exported by ${module}, which this pool thread is still evaluating (a pool thread evaluates the module of each export it runs). The goroutine would wait for the module, and the module for the goroutine. Run that module's top-level code only on the main thread, inside if (import.meta.main) { ... }, or move ${label} to a module of its own.`,
  );
}

// What in the arguments can't be cloned, and which argument holds it.
function uncloneableArgument(api: string, args: unknown[]) {
  for (let i = 0; i < args.length; i++) {
    const found = describe(args[i]);
    if (found === undefined) continue;
    const [path, what, hint] = found;
    return new DOMException(
      (path === ""
        ? `${api}() argument ${i + 1} is ${what}, which can't be sent to another thread.`
        : `${api}() argument ${i + 1} has ${what} at ${path}, which can't be sent to another thread.`) +
        (hint === undefined ? "" : ` ${hint[0].toUpperCase()}${hint.slice(1)}.`),
      "DataCloneError",
    );
  }
}

function argumentCloneError(api: string, error: unknown, args: unknown[]) {
  if ((error as Error)?.name !== "DataCloneError") return error;
  return uncloneableArgument(api, args) ?? error;
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
// would not all keep, the reason of the AbortSignal that was argument `index`, or another value as it was.
type Thrown = ["error", ErrorRecord] | ["signal", index: number] | ["value", unknown];

function fromThread(thrown: Thrown, signals: AbortSignal[] | undefined): unknown {
  if (thrown[0] === "signal") {
    const signal = signals?.[thrown[1]];
    return signal?.aborted ? signal.reason : new DOMException("This operation was aborted", "AbortError");
  }
  if (thrown[0] !== "error") return thrown[1];
  const record = thrown[1];
  const { name, message, stack, props, errors } = record;
  const options = "cause" in record ? { cause: fromThread(record.cause!, signals) } : undefined;
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
  } else if (errors !== undefined) {
    error = new AggregateError(
      errors.map(inner => fromThread(inner, signals)),
      message,
      options,
    );
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
  else entry.reject(outcome === Outcome.Rejected ? fromThread(value as Thrown, entry.signals) : value);
}

const kWaitGroupHint = "a WaitGroup belongs to the thread that created it: send results on a channel instead";

// A goroutine that waits for a free slot of a WaitGroup with a limit.
type Queued = { goroutine: Goroutine<unknown>; task: unknown; target: Target; args: unknown[] };

// Go's sync.WaitGroup with its Go method (Go 1.25), and errgroup's first-error cancellation and SetLimit.
class WaitGroup {
  #controller = new AbortController();
  #signal: AbortSignal;
  #running = new Set<Promise<unknown>>();
  // The first goroutine that failed, and whether wait() has thrown it.
  #failure: { error: unknown; thrown: boolean } | undefined;
  // What disposal aborted the group with. A goroutine that rejects with it (the reason of its copy of the
  // group's signal comes back as the original) did not fail.
  #disposal: DOMException | undefined;
  #limit = Infinity;
  // Goroutines started and not settled.
  #active = new Set<Promise<unknown>>();
  #queue: Queued[] = [];
  #queueHead = 0;

  constructor(options?: { signal?: AbortSignal; limit?: number }) {
    let parent: AbortSignal | undefined;
    if (options !== undefined) {
      if (!$isObject(options)) throw $ERR_INVALID_ARG_TYPE("options", "object", options);
      parent = options.signal;
      if (parent !== undefined && !(parent instanceof AbortSignal)) {
        throw $ERR_INVALID_ARG_TYPE("options.signal", "AbortSignal", parent);
      }
      const limit = options.limit;
      if (limit !== undefined) {
        if (typeof limit !== "number") throw $ERR_INVALID_ARG_TYPE("options.limit", "number", limit);
        if (!(limit >= 1) || (limit !== Infinity && !Number.isInteger(limit))) {
          throw $ERR_OUT_OF_RANGE("options.limit", "an integer >= 1, or Infinity", limit);
        }
        this.#limit = limit;
      }
    }
    // AbortSignal.any() holds what it makes weakly: a long-lived parent does not keep this alive.
    this.#signal = parent === undefined ? this.#controller.signal : AbortSignal.any([parent, this.#controller.signal]);
    // What waits for a slot rejects once the signal aborts, not once a slot frees.
    if (this.#limit !== Infinity) this.#signal.addEventListener("abort", () => this.#next(), { once: true });
    ownedByThisThread(this, kWaitGroupHint);
  }

  get signal() {
    return this.#signal;
  }

  go(fn: unknown, ...args: unknown[]) {
    const goroutine = Goroutine.create();
    try {
      const target = targetFor("WaitGroup.go", fn);
      if (this.#active.size < this.#limit) {
        this.#start(goroutine, fn, target, args);
      } else {
        // As go() would throw now, not once it starts.
        if (target[0] === TargetKind.Export && onPoolThread && evaluating(target[1])) {
          throw evaluatingHere("WaitGroup.go", fn, target[1], target[2]);
        }
        const uncloneable = uncloneableArgument("WaitGroup.go", args);
        if (uncloneable !== undefined) throw uncloneable;
        this.#queue.push({ goroutine, task: fn, target, args });
        if (this.#signal.aborted) this.#next();
      }
    } catch (error) {
      throw thrownBy(error, WaitGroup.prototype.go);
    }
    this.#running.add(goroutine);
    goroutine.then(
      () => this.#settled(goroutine),
      error => {
        if (this.#failure === undefined && (this.#disposal === undefined || error !== this.#disposal)) {
          this.#failure = { error, thrown: false };
          this.#controller.abort(error);
        }
        this.#settled(goroutine);
      },
    );
    return goroutine;
  }

  #start(goroutine: Goroutine<unknown>, task: unknown, target: Target, args: unknown[]) {
    start("WaitGroup.go", goroutine, task, target, args);
    this.#active.add(goroutine);
  }

  // Runs after a failure aborted the signal, so that what waits for the slot sees it.
  #settled(goroutine: Promise<unknown>) {
    this.#running.delete(goroutine);
    if (this.#active.delete(goroutine)) this.#next();
  }

  // Starts what waits for a slot. Once the group's signal has aborted, what waits rejects with its reason
  // instead of starting.
  #next() {
    const queue = this.#queue;
    while (this.#queueHead < queue.length && (this.#active.size < this.#limit || this.#signal.aborted)) {
      const { goroutine, task, target, args } = queue[this.#queueHead];
      queue[this.#queueHead++] = undefined!;
      if (this.#signal.aborted) {
        Goroutine.fail(goroutine, this.#signal.reason);
        continue;
      }
      try {
        this.#start(goroutine, task, target, args);
      } catch (error) {
        Goroutine.fail(goroutine, error);
      }
    }
    if (this.#queueHead === queue.length) {
      queue.length = 0;
      this.#queueHead = 0;
    }
  }

  async wait() {
    const running = this.#running;
    // Goroutines can start more while it waits.
    while (running.size > 0) await Promise.allSettled(running);
    const failure = this.#failure;
    if (failure !== undefined) {
      failure.thrown = true;
      throw failure.error;
    }
  }

  async [Symbol.asyncDispose]() {
    this.#disposal ??= new DOMException("The WaitGroup was disposed", "AbortError");
    this.#controller.abort(this.#disposal);
    const running = this.#running;
    while (running.size > 0) await Promise.allSettled(running);
    const failure = this.#failure;
    if (failure !== undefined && !failure.thrown) {
      failure.thrown = true;
      throw failure.error;
    }
  }
}

export default {
  go,
  settle,
  // internal/go/runtime, when it starts on a pool thread
  enterPool() {
    onPoolThread = true;
  },
  WaitGroup,
};
