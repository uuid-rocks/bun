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
