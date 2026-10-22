# Singer SDK taps

The SDK owns the BATCH plumbing: `Stream.sync()` calls `get_batch_config()`, and when it returns a config, `_sync_batches()` writes a BATCH message and then a STATE message for each `(encoding, manifest)` that `get_batches()` yields. Your job is to feed it Arrow data and keep state honest.

Check which SDK hook exists before writing code, by looking in the installed SDK (`.venv/lib/python*/site-packages/singer_sdk/streams/core.py`):

- `Stream.get_record_batches` exists → **native hook**.
- It doesn't → **custom `get_batches`**.

Either way, add `pyarrow` as a direct dependency. The SDK only pulls it in through its `parquet` extra.

## Native hook

Added by [meltano/sdk#3829](https://github.com/meltano/sdk/pull/3829). Until a release includes it, use it only from that branch.

Implement one method and let the SDK do the rest:

```python
class MyStream(Stream):
    replication_key = "id"
    is_sorted = True  # rows are ordered by the replication key

    def get_record_batches(self, context):
        start = self.get_starting_replication_key_value(context)
        for batch in client.fetch_arrow(query_after(start)):  # pa.RecordBatch | pa.Table
            yield cast_columns(batch)
```

The SDK rechunks to `batch_size`, drops deselected top-level columns, writes one IPC file per chunk through `batch_config.storage` (local or fsspec remote), and advances the bookmark from each file before its STATE message. Return `None` for streams that can't produce Arrow, and they fall back to `get_records()`.

## Custom `get_batches`

Use [tap-snowflake](https://github.com/MeltanoLabs/tap-snowflake) as the template: `get_batches_via_arrow_fetch` in `tap_snowflake/client.py`, and `rechunk` and `write_arrow_file` in `tap_snowflake/batch.py`. Mind these gotchas:

- **`batch_config` setting.** Before SDK 0.55 the SDK's `BATCH_CONFIG` schema only allows `jsonl` and `parquet`. Declare the setting in `tap.py` with `arrow` in `encoding.format`'s `allowed_values` (tap-snowflake `tap.py`).
- **Partial config.** Settings managers send `batch_config` with only defaults (`storage`, `batch_size`) to users who never enabled BATCH. Override `get_batch_config` to return `None` when `encoding` is missing, and apply defaults (`batch_size` 100000, `storage.root` `file://.batch_files`) there.
- **Starting value.** `get_starting_replication_key_value()` returns `None` inside `get_batches` unless you call `self._write_starting_replication_value(context)` first. Only the RECORD path initializes it.
- **State per file.** Yield one manifest per file as soon as it's written. Before yielding each file's `(encoding, [url])`, call `self._increment_stream_state({self.replication_key: last_value}, context=context)`. `_sync_batches` then emits the BATCH message and STATE, so an interrupted sync resumes after the last written file.
- **Max query.** Skip any replication-key max taken before the sync. Rows arriving during the sync push the per-file bookmarks past it, and the final state update then raises `InvalidStreamSortException`.
- **Rechunk, then write.** Keep these as two functions: a pure generator that re-slices driver output to `batch_size`, and a function that writes one chunk with `pyarrow.ipc.new_file` and returns its URL. Open files through `batch_config.storage.open(filename, "wb")` and build URLs with `storage.get_url(filename)`, so remote roots (`s3://`) work. tap-snowflake's `write_arrow_file` only supports local roots.
- **Selection.** Build the query from `self.get_selected_schema()["properties"]` (tap-snowflake `_build_select`), so RECORD and BATCH read the same columns.

## Log-based fallback (both paths)

Once `get_batch_config()` returns a config, the SDK routes the whole stream through BATCH. Return `None` from it for log-based streams so they keep emitting RECORD messages:

```python
def get_batch_config(self, config):
    if self.replication_method == "LOG_BASED":
        return None
    return super().get_batch_config(config)
```
