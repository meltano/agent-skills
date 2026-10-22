# Taps without the Singer SDK

`singer-python` and `pipelinewise-singer-python` taps have no BATCH support, so the tap owns everything: config parsing, the writer, the BATCH message and state. [meltano-tap-postgres](https://github.com/meltano/meltano-tap-postgres) is the cleanest template; [pipelinewise-tap-mysql](https://github.com/Matatika/pipelinewise-tap-mysql) is the first one and shows the MySQL-specific casts.

## Pieces

- **`batch_config` parsing.** A `BatchConfig.from_config()` that reads `encoding.format`, `storage.root` and `batch_size` (postgres `tap_postgres/batch.py`). Follow the SDK's shape, so the same Meltano settings work for SDK and non-SDK variants. Reject unsupported formats with a clear error.
- **Arrow source.** A thin module that opens the ADBC connection and returns a `RecordBatchReader` for a query (postgres `adbc.py` `stream_record_batches`, mysql `adbc.py`). In batch mode the tap opens no row-oriented connection at all (postgres `ArrowBatchSource`).
- **SQL generation.** Reuse the RECORD-mode query builder, adding the casts from the type map in the query (postgres `sqlgen.py`: `date` → `timestamp`, timestamp clamping; ADBC uses positional `$1` placeholders).
- **Writer.** An `ArrowBatchWriter` that buffers and slices batches to `batch_size`, writes one IPC file per chunk with `pyarrow.ipc.new_file`, and names files with a UUID.
- **Messages.** For each file, write `{"type": "BATCH", "stream": ..., "encoding": {"format": "arrow"}, "manifest": ["file://..."]}` with the library's message writer, then advance the bookmark from the file's last row and write STATE.
- **Dispatch.** In the full-table and incremental sync functions, branch to the Arrow path when batch mode is on (postgres `full_table.py` `_sync_table_arrow`, `incremental.py`). Leave the log-based sync and its initial snapshot unchanged.
- **Bookmark columns.** Columns that exist only for bookmarking (postgres `xmin`) are read for state and dropped from the table before it is written.

## Dependencies

`pyarrow` and `adbc-driver-manager` are core dependencies, so BATCH works without extras. The native ADBC driver is installed separately with `dbc install <driver>`. Pin it in a `dbc.toml`/`dbc.lock`, install it in CI (`make dbc` in tap-mysql), and give `require_arrow_support()` an error message that names the install command.
