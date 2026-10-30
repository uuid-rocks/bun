use core::ptr::NonNull;

use bun_collections::HashMap;

/// A VM's operations that wait for another thread, by the id the wake that settles one carries.
/// The wake holds only the id: an operation its VM gave up on (its context stopped, its signal
/// aborted) is gone from the table, and a late wake for it finds nothing. JS thread only.
pub(crate) struct OpTable<T> {
    last_id: u64,
    ops: HashMap<u64, NonNull<T>>,
}

impl<T> Default for OpTable<T> {
    fn default() -> Self {
        Self {
            last_id: 0,
            ops: HashMap::default(),
        }
    }
}

impl<T> OpTable<T> {
    /// An id no operation of this VM has had.
    pub(crate) fn next_id(&mut self) -> u64 {
        self.last_id += 1;
        self.last_id
    }

    /// The address stays the operation's until [`remove`](Self::remove).
    pub(crate) fn insert(&mut self, id: u64, op: Box<T>) -> NonNull<T> {
        let op = bun_core::heap::into_raw_nn(op);
        let previous = self.ops.insert(id, op);
        debug_assert!(previous.is_none());
        op
    }

    pub(crate) fn remove(&mut self, id: u64) -> Option<Box<T>> {
        let op = self.ops.remove(&id)?;
        // SAFETY: `insert` made it from a `Box`, and the table held it alone.
        Some(unsafe { bun_core::heap::take(op.as_ptr()) })
    }
}

impl<T> Drop for OpTable<T> {
    fn drop(&mut self) {
        // Each operation leaves when its context stops, which VM teardown does while the heap is
        // alive. One still here holds a handle into a heap that is gone: leak it.
        debug_assert!(
            self.ops.is_empty(),
            "an operation outlived its VM's contexts"
        );
    }
}
