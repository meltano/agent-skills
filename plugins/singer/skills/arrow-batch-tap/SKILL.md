---
name: arrow-batch-tap
description: Add Arrow BATCH support to a Singer tap, so it writes Arrow IPC files and emits BATCH messages instead of RECORD messages. Use when adding `batch_config`, Arrow, ADBC or BATCH messages to a tap, or benchmarking a tap's RECORD vs BATCH throughput.
---

# Arrow BATCH support for a Singer tap

The tap reads the source **natively as Arrow** (an ADBC driver, a client's `fetch_arrow_batches()`, PyMongoArrow, ...), writes Arrow IPC files of `batch_size` rows, and emits one `BATCH` message per file. The speedup comes from never building a Python dict per row, so every step below protects that: data stays columnar from the driver to the file.

## Invariants

These hold for every tap, whatever its framework:

- **Opt-in.** RECORD mode stays the default. BATCH is enabled through the `batch_config` setting:

  ```json
  "batch_config": {
    "encoding": {"format": "arrow"},
    "storage": {"root": "file://.batch_files"},
    "batch_size": 100000
  }
  ```

- **Log-based falls back to RECORD.** CDC (binlog, WAL, change streams) and the initial log-based snapshot keep emitting RECORD messages. Only FULL_TABLE and INCREMENTAL streams use BATCH.
- **One BATCH message per file, then STATE.** Write the file, emit its BATCH message, advance the bookmark to the file's last replication key value, and emit STATE. An interrupted sync then resumes from the last written file, and the target starts loading before extraction ends.
- **Columnar type fixes.** Fix type mismatches in the source query (a `CAST`) or with `pyarrow.compute` on whole columns, so the Arrow type matches what the stream's JSON Schema promises and what RECORD mode emits.
- **Native chunking is rechunked.** Driver chunks rarely match `batch_size`; slice and buffer them into files of exactly `batch_size` rows (the last may be smaller). The SDK's `get_record_batches` hook does this for you.

## Steps

1. **Identify the branch.** Read the tap's dependencies:
   - `singer-sdk` → read [`references/sdk-tap.md`](references/sdk-tap.md).
   - `singer-python`, `pipelinewise-singer-python` or no framework → read [`references/non-sdk-tap.md`](references/non-sdk-tap.md).

   Done when you have read the reference for the tap's branch.

2. **Choose the Arrow source.** Find how the source's driver yields Arrow, in order of preference: a native Arrow API of the existing client (reuses the connection, no new driver); an [ADBC](https://arrow.apache.org/adbc/) driver through `adbc-driver-manager`, with the driver installed by [`dbc`](https://docs.adbc-drivers.org/) and its install documented in the README; a source-specific Arrow library. Check that the library ships wheels for every Python version the tap supports, and note any `pyarrow` pin it carries. Import it next to the `pyarrow` version you'll install: Cython warnings like `pyarrow.lib.Array size changed, may indicate binary incompatibility` mean its wheels were built against another `pyarrow`, so pin the one they match (pymongoarrow 1.15.0 needs `pyarrow==25.0.0`, although it allows 25.0.x).

   Done when you can name the call that yields Arrow batches, every Python version in the tap's classifiers has a wheel, and importing it prints no binary-incompatibility warnings.

3. **Map types.** For every column type in the source, compare the Arrow type the driver yields with the stream's JSON Schema and with the value RECORD mode emits. Known mismatches from earlier taps:
   - integer-encoded booleans (MySQL `TINYINT(1)`) → `pa.bool_()`
   - `date` columns declared as `date-time` → cast to timestamp in the query
   - zero dates (`0000-00-00`) → null
   - identifier casing (Snowflake returns unquoted identifiers uppercased) → label columns with the schema property names
   - timestamp precision and time zone → match RECORD mode
   - driver extension types (e.g. PyMongoArrow's `ObjectId`, `Decimal128`) → plain Arrow types any target can read
   - free-form values (sub-documents, JSON columns, fields with mixed types) → Arrow's canonical JSON type, `pa.json_()`, holding the JSON text RECORD mode would emit. SDK targets (singer-sdk 0.55.0b7+) decode it back into the same values.

   For schema-less sources, don't let the driver infer types: PyMongoArrow fixes a field's type by its first value and silently turns later values of another type into nulls. Scan the types up front and decode with an explicit schema. Inside an Arrow struct, a missing field and a null one are both null; document that, since RECORD mode leaves missing fields out.

   **Check how strict the explicit schema really is.** Decode a value of every source type into every Arrow type you use, and list which mismatches raise and which are coerced silently: pymongoarrow truncates a double in an `int64` column (`1.5` becomes `1`) and turns booleans into `1`/`1.0`. Silent coercions need their own check, e.g. a server-side query per batch for those types.

   Done when every source type has a recorded Arrow type and a cast where it differs, and every silent coercion has a check.

4. **Implement** following the branch reference.

5. **Test.** Cover:
   - rechunking: buffering, exact `batch_size` boundaries, an empty source (skip if the SDK's `get_record_batches` hook does the rechunking)
   - `batch_config` parsing, including a partial config (no `encoding`)
   - an integration test against the real source (testcontainers or a CI service) that reads every file back through `singer_sdk.contrib.batch_decoder_arrow.ArrowReader`, the reader SDK targets use, and compares it to RECORD mode output for the same rows, covering every source type
   - resuming from the STATE emitted after a file
   - a type changing during the sync (update a row between the type check and extraction) failing loudly instead of being coerced
   - log-based streams still emitting RECORD messages

   Done when each bullet has a passing test.

6. **Document** in the README: the `batch_config` setting, which replication methods emit BATCH, and any driver installation step. Update every place the plugin's settings are declared outside the repo (Meltano Hub, `matatika-catalog`).

7. **Benchmark** in [melt-bench](https://github.com/meltano/melt-bench): add a `<source>_to_<dest>_record_<tier>.yml` and `<source>_to_<dest>_batch_<tier>.yml` scenario pair, the batch one differing only by `batch_config.encoding.format: arrow`. If the source has no melt-bench adapter yet, follow melt-bench's `CLAUDE.md` to add one.

   Done when both scenarios have a committed result in `benchmark-results/`.

## Reference implementations

| Tap | Framework | Arrow source | Code |
|---|---|---|---|
| [tap-mongodb](https://github.com/MeltanoLabs/tap-mongodb) | Singer SDK, native hook | PyMongoArrow on `find_raw_batches` | `tap_mongodb/streams.py` (`get_record_batches`, `get_batch_config`), `arrow.py` |
| [tap-snowflake](https://github.com/MeltanoLabs/tap-snowflake) | Singer SDK | `fetch_arrow_batches()` | `tap_snowflake/client.py` (`get_batches`), `batch.py` |
| [meltano-tap-postgres](https://github.com/meltano/meltano-tap-postgres) | singer-python | ADBC | `tap_postgres/batch.py`, `adbc.py`, `sqlgen.py` |
| [pipelinewise-tap-mysql](https://github.com/Matatika/pipelinewise-tap-mysql) | pipelinewise-singer-python | ADBC via `dbc` | `tap_mysql/adbc.py`, `sync_strategies/common.py` |
