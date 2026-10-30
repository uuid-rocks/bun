type Channel = import("bun").Channel<unknown>;

// `break` in `for await` ends the loop, not the channel: other receivers keep receiving.
$overriddenName = "[Symbol.asyncIterator]";
export function asyncIterator(this: Channel) {
  const channel = this;
  return {
    next() {
      return channel.$channelNext();
    },
    return(value) {
      return Promise.$resolve({ value, done: true });
    },
    [Symbol.asyncIterator]() {
      return this;
    },
  };
}

// `console.log(channel)` shows its state, as for other Bun classes, not its methods.
$overriddenName = "[nodejs.util.inspect.custom]";
export function inspectCustom(this: Channel, depth: number, options) {
  let fields;
  try {
    fields = { capacity: this.capacity, length: this.length, closed: this.closed };
  } catch {
    // What inherits from a channel, such as its prototype
    return this;
  }
  if (depth < 0) return options.stylize("[Channel]", "special");
  const { inspect } = require("node:util");
  const opts = { ...options, depth: options.depth == null ? null : options.depth - 1 };
  return `Channel ${inspect(fields, opts)}`;
}
