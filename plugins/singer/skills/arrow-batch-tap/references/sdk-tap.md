# Singer SDK taps

The SDK owns the BATCH plumbing: `Stream.sync()` calls `get_batch_config()`, and when it returns a config, `_sync_batches()` writes a BATCH message and then a STATE message for each `(encoding, manifest)` that `get_batches()` yields. Your job is to feed it Arrow data and keep state honest.

Pick the path by SDK version:

- `singer-sdk>=0.55.0b5` → **native hook**. Prefer it: bump the tap's pin if you can. Because it's a pre-release, write the specifier with the pre-release version (`singer-sdk>=0.55.0b5`), so pip and uv will resolve it.
- Older SDKs → **custom `get_batches`**.

Either way, add `pyarrow` as a direct dependency. The SDK only pulls it in through its `parquet` extra.

## Native hook

`Stream.get_record_batches` ([meltano/sdk#3829](https://github.com/meltano/sdk/pull/3829)). Implement one method and let the SDK do the rest:

```python
class MyStream(Stream):
    replication_key = "id"
    is_sorted = True  # rows are ordered by the replication key

    def get_record_batches(self, context):
        start = self.get_starting_replication_key_value(context)
        if start is None:  # First sync
            return client.fetch_arrow("SELECT * FROM t ORDER BY id")
        # `>=`: rows sharing the bookmark's value may span two files
        return client.fetch_arrow("SELECT * FROM t WHERE id >= ? ORDER BY id", start)
```

Yield or return `pa.RecordBatch` and `pa.Table` objects, in any chunk sizes. The SDK then:

- rechunks to `batch_size` and writes one IPC file per chunk through `batch_config.storage` (local or fsspec remote);
- drops deselected top-level columns, and handles columns missing from the schema like RECORD mode: dropped with a warning, unless `TYPE_CONFORMANCE_LEVEL` is `NONE` or the schema allows `additionalProperties`;
- advances state to each file's max replication-key value before its STATE message. Sorted streams resume after the last written file; unsorted ones keep progress markers until the sync completes, as in RECORD mode;
- calls the hook once per partition, with that partition's starting value and state;
- honours `ABORT_AT_RECORD_COUNT` (dry runs), truncating the file that crosses it.

What stays the tap's job:

- **Order.** With `is_sorted = True`, the SDK only checks order between files, not inside one, so the source query must sort by the replication key.
- **Types.** Cast columns to match the stream schema before yielding (see the type map step in `SKILL.md`).
- **What's skipped.** Stream maps and `post_process` aren't applied to Arrow tables.
- **Fallbacks.** Streams with child streams, and deselected parents synced for their children, use `get_records()`, because only records produce child contexts. Return `None` for every partition to use `get_records()` too.

SDK 0.55 also accepts `arrow` in its built-in `batch_config` setting and ignores a partial config without `encoding`, so neither needs overriding. Its defaults are `batch_size` 10000 and a `singer-sdk` folder in the OS temp directory. Override `get_batch_config` only if the tap wants different ones (tap-snowflake uses 100000 and `file://.batch_files`).

## Custom `get_batches`

For SDKs older than 0.55.0b5. Use [tap-snowflake](https://github.com/MeltanoLabs/tap-snowflake) as the template: `get_batches_via_arrow_fetch` in `tap_snowflake/client.py`, and `rechunk` and `write_arrow_file` in `tap_snowflake/batch.py`. Mind these gotchas:

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
