# KwelaDB: what should become general graphio later

graphio's KwelaDB support lives in one experimental module, `graphio/kweladb.py`, plus a hook in
`NodeSet`/`RelationshipSet` `create()` and `merge()`. It is off unless `GRAPHIO_KWELA_FLIGHT` is
set, and it is tested only in `test_kweladb/` (outside `testpaths`, never in CI; run with
`make test-kweladb` against a KwelaDB started elsewhere).

Some of it isn't KwelaDB-specific. It is there because putting it into graphio's core means a
public API, full tests against Neo4j in CI, and docs. This file lists those parts, where they
should go, and what moving them needs.

## 1. Arrow sources for NodeSet and RelationshipSet

**Now:** `kweladb.create_nodes(node_set, driver, rows)`, `merge_nodes`, `create_relationships`,
`merge_relationships`. `rows` is any Arrow source: a pyarrow `Table`, `RecordBatch` or
`RecordBatchReader`, an iterable of record batches (`ParquetFile.iter_batches()`), or any object
with `__arrow_c_stream__` (a DuckDB result, a Polars data frame). Read as a stream. Over Flight
the batches are uploaded as they are; otherwise (Neo4j, or Flight off) they become dicts sent
with UNWIND in transactions of `batch_size`. Helpers: `_reader`, `_source_batches`,
`_relationship_batches`, `_row_batches`, `_check`, `_normalize`.

Nothing in it depends on KwelaDB except the upload: loading DuckDB or Parquet data into Neo4j
without building Python dicts first is useful on its own.

**Should become:** a source parameter on the existing methods,
`NodeSet.create(driver, rows=source)`, `NodeSet.merge(driver, rows=source)`, the same on
`RelationshipSet`, with the helpers in a new module `graphio/bulk/arrow.py`. The KwelaDB
functions then call those (or go away).

**Needs:**
- an extra `arrow = ["pyarrow>=24"]`; the `kweladb` extra becomes `arrow` plus
  `adbc-driver-flightsql`;
- pyarrow in the dev extra, so CI can run the new tests;
- tests in `test/bulk/` against Neo4j (Community and Enterprise, as the rest): every source kind,
  `create` and `merge`, `append_props`, `preserve`, `default_props`, explicit nulls (`+=`
  removes), dates and timestamps, dictionary-encoded columns, type errors, the relationship shape
  (`start_<key>`, `end_<key>`, properties), batch splitting across Arrow batches, databases;
  the unit tests in `test_kweladb/test_arrow.py` (the Arrow source part) move with it;
- decisions:
  - the relationship convention: `start_<key>`/`end_<key>` column names (as graphio's query
    parameters), or a mapping argument (`start={'usid': 'source_usid'}`);
  - deduplication: an Arrow source isn't deduplicated (`NodeSet(deduplicate=True)` applies to
    `add_node` only);
  - the OGM: whether `NodeModel` gets an Arrow path at all;
- docs: a section in the bulk loading guide.

## 2. Exact conversion of dict rows to Arrow

**Now:** `kweladb.to_arrow(rows)` / `arrow_table(rows)`: a set's dict rows as an Arrow table
with exact types (no int-to-float widening), or the reason it can't be done (`Inexact`). Used to
upload a set's own rows over Flight.

**Should become:** `NodeSet.to_arrow()` / `RelationshipSet.to_arrow()` in the bulk modules, e.g.
to write a set to Parquet (`pq.write_table(ns.to_arrow(), path)`) next to the existing JSON
serialization.

**Needs:** the same tests in `test/bulk/`, no server; a decision on how missing keys and `None`
map to nulls when writing files (today: a null means `None` if all rows have the same keys).

## 3. Type rules

**Now:** two places decide what graphio sends: `_kind`/`_array` (dict rows → Arrow) and `_check`
(Arrow sources). Both accept booleans, integers, floats, strings, dates, timestamps and times
(Arrow sources), and lists of these, and reject the rest with the column named.

**Should become:** one module with the type mapping between Python, Arrow and Neo4j values,
used by both, and documented, so users know what a DuckDB or Parquet column becomes in the graph.

## 4. One log line per set

**Now:** `graphio.kweladb` logs one INFO line per set: rows, transport (Arrow Flight or UNWIND),
time, and why a set didn't go over Flight.

**Should become:** the same line for every bulk write, on the UNWIND path too (rows, batches,
rows/s), from `graphio/bulk/` (logger `graphio.bulk`). Useful for any pipeline, not only with
KwelaDB.

**Needs:** a decision on the level (INFO may be too chatty for small OGM writes; the KwelaDB code
logs sets under `MIN_ROWS` at DEBUG for that reason).

## 5. Upload batches by size

**Now:** `kweladb.batches(table, limit)` splits a table into record batches of at most 8 MB,
measured (rows of very different sizes, e.g. protein sequences, broke an estimate).

**Should become:** part of `graphio/bulk/arrow.py` (item 1) if other Arrow transports come; for
now only Flight needs it.

## What stays KwelaDB-specific

- the switch `GRAPHIO_KWELA_FLIGHT`, the protocol check (`kwela.protocol()`), `MIN_ROWS`;
- the Flight upload (`_upload`, the `kwela-database` header) and the staged-table query
  (`CALL kwela.staged(...)`, `keepNulls`, `kwela.dropStaged`).

## Order

Item 1 first: it changes graphio's API for everyone, so it gets the full tests and docs above.
Items 2 to 5 can follow when they are needed.
