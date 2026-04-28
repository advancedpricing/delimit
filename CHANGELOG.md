# Changelog

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
