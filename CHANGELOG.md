# Changelog

## 0.5.0 (2026-10-28)

### Features

- Add the `validate_headers: true` read option for `read/2`, `read_string/2`
  and `stream/2`. It checks the file's header against the schema's headers
  (field labels, or field names when there is no label) and raises
  `Delimit.HeaderError` when they differ. Headers match ignoring case and
  surrounding whitespace, a UTF-8 byte order mark before the first header is
  ignored, and the order must match, since `stream/2` reads columns by
  position. The option requires `headers: true` and is off by default, so
  existing callers are unaffected. `stream/2` checks the header when it is
  called rather than on the first read.
- Add `Delimit.HeaderError`, carrying the `expected` and `found` headers plus
  the `missing` and `unexpected` ones, with a message naming them. Its
  `check/2` compares a header row without raising.

### Bug Fixes

- `read/2` and `read_string/2` with `headers: true` find columns by header
  name, but did not trim the header row or drop a UTF-8 byte order mark from
  it. A header with spaces around it, or the first header of a file saved with
  a byte order mark, matched no field, so that column was read as `nil`. Both
  are now cleaned before columns are matched.

## 0.4.2 (2026-05-15)

### Bug Fixes

- Fix `read_options` and `write_options` typespecs: the option was
  declared as `:header` (singular) but every code path reads `:headers`
  (plural). Callers passing `headers: true | false` were breaking the
  type contract, which Dialyzer propagated through the macro-generated
  `stream/2` and `read/2`, poisoning analysis in consumer projects with
  cascades of `no_return` and `unused_fun` warnings. Renames `:header`
  to `:headers` in both types and docstrings.

## 0.4.1 (2026-04-28)

### Bug Fixes

- `stream/2` was silently dropping the first data row in headerless files.
  The streaming path called `NimbleCSV.parse_stream/1` without forwarding
  the `:headers` option, so NimbleCSV's default `skip_headers: true` ate
  the first line. Now `headers: false` (or unset) preserves every row,
  and `headers: true` skips the first row — matching `read_string/2`.

## 0.4.0 (2026-04-28)

### Features

- Add `formats:` option for `:date` and `:datetime` fields. Tries each
  format in order on read; the first to parse successfully wins. Useful
  for files that mix date formats (e.g. mostly `M/D/YYYY` with occasional
  `YYYY-MM-DD`). Mutually exclusive with `format:`. Writing uses the
  first entry in the list.
- Add `Module.canonical_string/2` (and `Delimit.Schema.canonical_string/3`):
  generate a stable, deterministic string encoding of a struct based on
  its schema. Uses ASCII Unit Separator as the default delimiter.
- Add `Module.row_hash/2` (and `Delimit.Schema.row_hash/3`): compute a
  cryptographic hash of a struct's canonical encoding. Defaults to SHA-256
  truncated to 16 bytes.
- Add `:row_hash` derived field type. Declared in the layout, populated
  automatically on read with the hash of the row's non-derived fields.
  Skipped on write. Supports `algorithm:` and `truncate:` options.
- Add `:raw_row` derived field type. Captures the row as a list of strings
  before type coercion. Useful for debugging and audit trails. Skipped
  on write.

## 0.3.0 (2026-03-03)

### Features

- Add fixed-width format support (`:fixed_width`)

## 0.2.0 (2025-05-21)

### Fixes

- Do not double skip headers

### Other

- Simplified code to eliminate pre-processing
- More appropriate empty row/value detection
- Rely on layout for field mapping, not layout AND header content

## 0.1.0 (2025-05-12)

- Initial release
