//! `Bun.go(fn)` with a function no module exports (BunGo.cpp): what its text uses that is not its
//! own, which would not exist on the thread it runs on.

use core::ffi::c_void;

use bun_core::strings;
use bun_js_parser::parse::parse_entry::function_free_names;

/// Zero-based, in bytes of the text; the line is -1 for none.
#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct Position {
    line: i32,
    column: i32,
}

const NONE: Position = Position {
    line: -1,
    column: 0,
};

/// BunGo.cpp's `FunctionNames`.
#[repr(C)]
pub(crate) struct FunctionNames {
    /// `bun_js_parser`'s `FunctionTextKind`.
    kind: u8,
    this: Position,
    import_meta: Position,
    direct_eval: bool,
}

/// Calls `on_name(ctx, name, len, position)` for each identifier `text` uses that nothing in it
/// declares, in order. Returns false if `text` does not parse.
///
/// # Safety
/// `text` points to `len` bytes of UTF-8.
#[unsafe(no_mangle)]
pub(crate) unsafe extern "C" fn Bun__Go__functionNames(
    text: *const u8,
    len: usize,
    ctx: *mut c_void,
    on_name: unsafe extern "C" fn(*mut c_void, *const u8, usize, Position),
    out: &mut FunctionNames,
) -> bool {
    // SAFETY: the caller's contract.
    let text = unsafe { core::slice::from_raw_parts(text, len) };
    let Some(names) = function_free_names(text) else {
        return false;
    };
    let position_of = |at: u32| -> Position {
        let before = &text[..(at as usize).min(text.len())];
        let line_start = strings::last_index_of_char(before, b'\n').map_or(0, |i| i + 1);
        Position {
            line: strings::count_char(before, b'\n') as i32,
            column: (before.len() - line_start) as i32,
        }
    };
    out.kind = names.kind as u8;
    out.this = names.this.map_or(NONE, position_of);
    out.import_meta = names.import_meta.map_or(NONE, position_of);
    out.direct_eval = names.direct_eval;
    for (name, at) in &names.free {
        // SAFETY: `on_name` reads `name` during the call only.
        unsafe { on_name(ctx, name.as_ptr(), name.len(), position_of(*at)) };
    }
    true
}
