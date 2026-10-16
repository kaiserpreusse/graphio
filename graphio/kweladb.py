"""
Experimental: bulk loads into KwelaDB over Arrow Flight SQL.

KwelaDB (an analytical graph server that speaks Neo4j's protocol) takes bulk data as Arrow
tables over Flight SQL. With this switched on, :class:`~graphio.NodeSet` and
:class:`~graphio.RelationshipSet` ``create()`` and ``merge()`` send their rows to KwelaDB as one
Arrow table and run the same Cypher as always over Bolt, reading the rows with
``CALL kwela.staged(...)`` instead of ``UNWIND $props``. Results are the same; only the
transport changes.

Switch it on with an environment variable and the ``kweladb`` extra::

    pip install graphio[kweladb]
    export GRAPHIO_KWELA_FLIGHT=grpc://localhost:8815      # or grpc://user:password@host:port

Without the variable, graphio runs exactly as before. With it, graphio still uses ``UNWIND``

- for sets of fewer than ``MIN_ROWS`` rows (1,000; ``GRAPHIO_KWELA_FLIGHT_MIN_ROWS`` sets
  another), where a round trip costs more than it saves,
- on servers that aren't KwelaDB (or speak another version of its bulk protocol),
- for rows Arrow can't carry exactly: a key with values of different types (an int in one row,
  a float in the next), other types than bool, int, float, str, date and lists of those, or rows
  that have both missing keys and explicit ``None`` values.

Unlike ``UNWIND``, a set is sent and written as a whole, not in batches of ``batch_size``.
"""

import datetime
import logging
import os
import time
import uuid
import warnings
import weakref
from itertools import chain
from urllib.parse import unquote, urlsplit

log = logging.getLogger(__name__)

#: The environment variable that switches the Flight transport on.
ENV_VAR = 'GRAPHIO_KWELA_FLIGHT'
#: The version of KwelaDB's bulk protocol this module speaks (``CALL kwela.protocol()``).
PROTOCOL = 1
#: Smaller sets use UNWIND: a Flight round trip costs more than it saves.
MIN_ROWS = int(os.environ.get('GRAPHIO_KWELA_FLIGHT_MIN_ROWS', 1000))

# Arrow's gRPC messages must stay under 16 MB: batches of about 8 MB.
_BATCH_BYTES = 8 * 1024 * 1024

_servers = weakref.WeakKeyDictionary()
_warned = False


def enabled() -> bool:
    """Whether the Flight transport is switched on (``GRAPHIO_KWELA_FLIGHT`` is set)."""
    return bool(os.environ.get(ENV_VAR))


def bulk(
    driver, database, query: str, parameter: str, rows: list, params: dict, lists=(), name='rows'
) -> bool:
    """
    Run ``query``, built for ``UNWIND $<parameter> AS ...``, over ``rows`` sent with Flight.

    Logs one INFO line per set (logger ``graphio.kweladb``): how it was sent, and if not over
    Flight, why.

    :param params: The query's other parameters.
    :param lists: Keys (of the rows, or of their ``properties`` maps) that the query only reads
        as ``[] + value`` (``append_props``): their scalars may be sent as one-element lists.
    :return: True if it ran; False if the caller should run it with ``UNWIND`` (the Flight
        transport is off, the server isn't KwelaDB, or the rows can't be sent exactly).
    :param name: The set, for the log (e.g. ``Gene (merge)``).
    """
    if not enabled():
        return False
    if len(rows) < MIN_ROWS:
        # Debug only: the OGM writes single objects this way.
        log.debug('%s: %s rows over UNWIND (fewer than %s)', name, f'{len(rows):,}', MIN_ROWS)
        return False
    prefix = f'UNWIND ${parameter} AS '
    if not query.startswith(prefix):
        return False
    if not _speaks_protocol(driver):
        return False
    try:
        import adbc_driver_flightsql  # noqa: F401
        import pyarrow  # noqa: F401
    except ImportError as e:
        raise ImportError(
            f'{ENV_VAR} needs pyarrow and adbc-driver-flightsql: pip install graphio[kweladb]'
        ) from e
    started = time.perf_counter()
    try:
        table, keep_nulls = arrow_table(rows, lists)
    except Inexact as reason:
        log.info('%s: %s rows over UNWIND: %s', name, f'{len(rows):,}', reason)
        return False
    _warn_once()

    staged_name = f'graphio_{uuid.uuid4().hex}'
    _upload(os.environ[ENV_VAR], database, staged_name, table)
    uploaded = time.perf_counter()
    _run_staged(driver, database, query[len(prefix) :], staged_name, keep_nulls, params)
    done = time.perf_counter()
    log.info(
        '%s: %s rows over Arrow Flight in %.1f s (conversion and upload %.1f s)',
        name,
        f'{len(rows):,}',
        done - started,
        uploaded - started,
    )
    return True


class Inexact(ValueError):
    """Arrow can't carry the rows exactly; the message says why."""


def bulk_arrow(driver, database, query: str, parameter: str, arrow_batches, params: dict,
               name='rows') -> bool:  # fmt: skip
    """
    Run ``query``, built for ``UNWIND $<parameter> AS ...``, over Arrow record batches sent as
    they are (an Arrow source, :mod:`graphio.bulk.arrow`): no Python rows, streamed.

    :param arrow_batches: Normalized batches in the query's row shape; consumed only if the
        Flight transport is on and the server speaks KwelaDB's bulk protocol.
    :return: True if it ran; False if the caller should send the batches with UNWIND.
    """
    if not enabled():
        return False
    prefix = f'UNWIND ${parameter} AS '
    if not query.startswith(prefix) or not _speaks_protocol(driver):
        return False
    try:
        import adbc_driver_flightsql  # noqa: F401
        import pyarrow as pa
    except ImportError as e:
        raise ImportError(
            f'{ENV_VAR} needs pyarrow and adbc-driver-flightsql: pip install graphio[kweladb]'
        ) from e

    started = time.perf_counter()
    first = next(iter(arrow_batches), None)
    if first is None:
        log.info('%s: no rows', name)
        return True
    _warn_once()
    rows = 0

    def limited():
        nonlocal rows
        for batch in chain([first], arrow_batches):
            rows += batch.num_rows
            yield from batches(pa.Table.from_batches([batch]))

    staged_name = f'graphio_{uuid.uuid4().hex}'
    _upload(
        os.environ[ENV_VAR],
        database,
        staged_name,
        pa.RecordBatchReader.from_batches(first.schema, limited()),
    )
    uploaded = time.perf_counter()
    _run_staged(driver, database, query[len(prefix) :], staged_name, True, params)
    log.info(
        '%s: %s rows over Arrow Flight in %.1f s (upload %.1f s)',
        name,
        f'{rows:,}',
        time.perf_counter() - started,
        uploaded - started,
    )
    return True


def _run_staged(driver, database, rest: str, staged_name: str, keep_nulls: bool, params: dict):
    """The query's rest over the staged table, then the table dropped."""
    staged = 'CALL kwela.staged($kwela_table, $kwela_options) YIELD row AS ' + rest
    options = {'keepNulls': keep_nulls}

    def run(tx):
        tx.run(staged, kwela_table=staged_name, kwela_options=options, **params).consume()

    with driver.session(database=database) as session:
        try:
            session.execute_write(run)
        finally:
            try:
                drop = 'CALL kwela.dropStaged($kwela_table)'
                session.run(drop, kwela_table=staged_name).consume()
            except Exception as e:  # the load's own error matters more
                log.warning('could not drop staged table %s: %s', staged_name, e)


def to_arrow(rows: list, lists=()):
    """
    The rows (maps, with at most one level of nested maps) as an Arrow table, and whether its
    nulls are explicit ``None`` values (all rows have the same keys) or missing keys.

    :param lists: Keys whose scalars may become one-element lists (see :func:`bulk`).
    :return: ``(table, keep_nulls)``, or None if Arrow can't carry the rows exactly.
    """
    try:
        return arrow_table(rows, lists)
    except Inexact:
        return None


def arrow_table(rows: list, lists=()):
    """:func:`to_arrow`, raising :class:`Inexact` with the reason instead of returning None."""
    import pyarrow as pa

    keys = {}
    nested = {}
    first = None
    uniform = True
    has_none = False
    for row in rows:
        if not isinstance(row, dict):
            raise Inexact(f'a row is a {type(row).__name__}, not a map')
        signature = []
        for key, value in row.items():
            keys.setdefault(key, None)
            if isinstance(value, dict):
                fields = nested.setdefault(key, {})
                for k, v in value.items():
                    fields.setdefault(k, None)
                    has_none = has_none or v is None
                signature.append((key, tuple(sorted(value))))
            else:
                has_none = has_none or value is None
                signature.append((key, None))
        signature.sort()
        if first is None:
            first = signature
        elif signature != first:
            uniform = False
    if not uniform and has_none:
        # A null would mean "missing" in some rows and "None" in others.
        raise Inexact('some rows lack keys that others have, and some values are None')

    columns = []
    for key in keys:
        values = [row.get(key) for row in rows]
        if key in nested:
            if any(v is not None and not isinstance(v, dict) for v in values):
                raise Inexact(f"key '{key}' holds a map in some rows and other values in others")
            if not nested[key]:
                # Maps without keys (relationships without properties).
                array = pa.array([None if v is None else {} for v in values], type=pa.struct([]))
                columns.append(array)
                continue
            children = []
            for field in nested[key]:
                column = [None if v is None else v.get(field) for v in values]
                children.append(_array(pa, column, f'{key}.{field}', field in lists))
            mask = pa.array([v is None for v in values])
            array = pa.StructArray.from_arrays(children, names=list(nested[key]), mask=mask)
        else:
            array = _array(pa, values, key, key in lists)
        columns.append(array)
    return pa.Table.from_arrays(columns, names=list(keys)), uniform


def _kind(value):
    """The Python type that decides the Arrow type; None if not supported."""
    if isinstance(value, bool):
        return bool
    for kind in (int, float, str):
        if isinstance(value, kind):
            return kind
    if type(value) is datetime.date:
        return datetime.date
    return None


def _name(kind) -> str:
    return 'date' if kind is datetime.date else kind.__name__


def _array(pa, values, key: str, as_lists=False):
    """The values as one Arrow array of one exact type; raises Inexact otherwise."""
    if as_lists and any(isinstance(v, list) for v in values):
        values = [v if v is None or isinstance(v, list) else [v] for v in values]
    scalar = element = None
    lists = False
    for value in values:
        if value is None:
            continue
        if isinstance(value, list):
            lists = True
            for item in value:
                if item is None:
                    continue
                kind = _kind(item)
                if kind is None:
                    raise Inexact(f"key '{key}' has lists with {type(item).__name__} items")
                if element not in (None, kind):
                    raise Inexact(
                        f"key '{key}' has lists with {_name(element)} and {_name(kind)} items"
                    )
                element = kind
        else:
            kind = _kind(value)
            if kind is None:
                raise Inexact(f"key '{key}' has {type(value).__name__} values")
            if scalar not in (None, kind):
                raise Inexact(f"key '{key}' has {_name(scalar)} and {_name(kind)} values")
            scalar = kind
    if lists and scalar is not None:
        raise Inexact(f"key '{key}' has lists in some rows and single values in others")
    types = {
        bool: pa.bool_(),
        int: pa.int64(),
        float: pa.float64(),
        str: pa.string(),
        datetime.date: pa.date32(),
    }
    arrow_type = pa.list_(types[element or str]) if lists else types[scalar or str]
    try:
        return pa.array(values, type=arrow_type)
    except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError) as error:
        raise Inexact(f"key '{key}': {error}") from None


# ---- Arrow sources ---------------------------------------------------------------------------
#
# Experimental, here until graphio has them in general (KWELADB_GENERALIZE.md): a NodeSet's or
# RelationshipSet's rows from Arrow data instead of Python dicts. A source is anything that
# speaks Arrow: a pyarrow Table, RecordBatch or RecordBatchReader, an iterable of record batches
# (ParquetFile.iter_batches()), or an object with __arrow_c_stream__ (a DuckDB result, a Polars
# data frame). Read as a stream. Node rows: one column per property. Relationship rows:
# start_<key> and end_<key> columns for the endpoints, every other column a property. A null is
# an explicit None. The set's default_props fill in missing columns.
#
# Over Arrow Flight (GRAPHIO_KWELA_FLIGHT, a KwelaDB server) the batches are uploaded as they
# are; otherwise (Neo4j, or the transport off) they become dicts sent with UNWIND in transactions
# of batch_size, as a set's own rows are.


def create_nodes(node_set, driver, rows, database=None, batch_size=None):
    """Create `node_set`'s nodes from an Arrow source `rows` (see above)."""
    from graphio.bulk.nodeset import nodes_create_factory

    q = nodes_create_factory(
        node_set.labels, property_parameter='props', additional_labels=node_set.additional_labels
    )
    name = f'{":".join(node_set.labels or [])} (create)'
    _write_nodes(node_set, driver, database, q, {}, batch_size, rows, name)


def merge_nodes(node_set, driver, rows, database=None, batch_size=None):
    """Merge `node_set`'s nodes from an Arrow source `rows` on its merge keys, with its
    append_props and preserve (see above)."""
    from graphio.bulk.nodeset import nodes_merge_factory

    if not node_set.merge_keys:
        raise ValueError('Merge keys are empty, MERGE requires merge keys.')
    q = nodes_merge_factory(
        node_set.labels,
        node_set.merge_keys,
        array_props=node_set.append_props,
        preserve=node_set.preserve,
        property_parameter='props',
        additional_labels=node_set.additional_labels,
    )
    params = {'append_props': node_set.append_props, 'preserve': node_set.preserve}
    name = f'{":".join(node_set.labels or [])} (merge)'
    _write_nodes(node_set, driver, database, q, params, batch_size, rows, name)


def create_relationships(rel_set, driver, rows, database=None, batch_size=None):
    """Create `rel_set`'s relationships from an Arrow source `rows` (see above)."""
    from graphio.bulk.relationshipset import rels_create_factory

    q = rels_create_factory(
        rel_set.start_node_labels,
        rel_set.end_node_labels,
        rel_set.start_node_properties,
        rel_set.end_node_properties,
        rel_set.rel_type,
    )
    _write_relationships(
        rel_set, driver, database, q, {}, batch_size, rows, f'{rel_set.rel_type} (create)'
    )


def merge_relationships(rel_set, driver, rows, database=None, batch_size=None):
    """Merge `rel_set`'s relationships from an Arrow source `rows`, with its append_props."""
    from graphio.bulk.relationshipset import rels_merge_factory

    q = rels_merge_factory(
        rel_set.start_node_labels,
        rel_set.end_node_labels,
        rel_set.start_node_properties,
        rel_set.end_node_properties,
        rel_set.rel_type,
        append_props=rel_set.append_props,
    )
    params = {'append_props': rel_set.append_props}
    _write_relationships(
        rel_set, driver, database, q, params, batch_size, rows, f'{rel_set.rel_type} (merge)'
    )


def _write_nodes(node_set, driver, database, q, params, batch_size, rows, name):
    source = _source_batches(rows, defaults=node_set.default_props)
    _write(driver, database, q, 'props', params, batch_size, source, name)


def _write_relationships(rel_set, driver, database, q, params, batch_size, rows, name):
    source = _relationship_batches(
        rows,
        [str(p) for p in rel_set.start_node_properties],
        [str(p) for p in rel_set.end_node_properties],
        defaults=rel_set.default_props,
    )
    _write(driver, database, q, 'rels', params, batch_size, source, name)


def _write(driver, database, q, parameter, params, batch_size, source, name):
    """Normalized Arrow batches over Flight, or as dicts with UNWIND."""
    if bulk_arrow(driver, database, q, parameter, source, params, name=name):
        return
    from graphio.utils import BATCHSIZE

    def write_batch(tx, batch):
        tx.run(q, {parameter: batch, **params}).consume()

    started, rows = time.perf_counter(), 0
    with driver.session(database=database) as session:
        for batch in _row_batches(source, batch_size or BATCHSIZE):
            session.execute_write(write_batch, batch)
            rows += len(batch)
    log.info(
        '%s: %s rows from Arrow over UNWIND in %.1f s',
        name,
        f'{rows:,}',
        time.perf_counter() - started,
    )


def _pa():
    try:
        import pyarrow as pa
    except ImportError as e:
        raise ImportError('Arrow sources need pyarrow: pip install graphio[kweladb]') from e
    return pa


def _reader(source):
    """The source as a ``pyarrow.RecordBatchReader``."""
    pa = _pa()
    if isinstance(source, pa.RecordBatchReader):
        return source
    if isinstance(source, pa.RecordBatch):
        return pa.RecordBatchReader.from_batches(source.schema, [source])
    if hasattr(source, '__arrow_c_stream__'):
        return pa.RecordBatchReader.from_stream(source)
    try:
        batches = iter(source)
    except TypeError:
        raise TypeError(
            f'not an Arrow source: {type(source).__name__} (a pyarrow Table, RecordBatch, '
            'RecordBatchReader, an iterable of record batches, or an object with '
            '__arrow_c_stream__)'
        ) from None
    first = next(batches, None)
    if first is None:
        raise ValueError(
            'an empty iterable has no schema: pass a pyarrow Table or RecordBatchReader'
        )
    if not isinstance(first, pa.RecordBatch):
        raise TypeError(f'an iterable of record batches yielded {type(first).__name__}')
    return pa.RecordBatchReader.from_batches(first.schema, chain([first], batches))


def _decoded_type(pa, arrow_type):
    """The type a column is sent as: dictionaries decoded (DuckDB enums, Parquet dictionaries)."""
    if pa.types.is_dictionary(arrow_type):
        return _decoded_type(pa, arrow_type.value_type)
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return pa.list_(_decoded_type(pa, arrow_type.value_type))
    return arrow_type


def _check(pa, name, arrow_type, nested=False):
    """Types both transports carry exactly: booleans, integers (but uint64), floats (but half
    floats), strings, dates, timestamps, times, nulls, and lists of these."""
    t = pa.types
    if t.is_list(arrow_type) or t.is_large_list(arrow_type):
        if nested:
            raise TypeError(f"column '{name}': lists of lists can't be properties")
        return _check(pa, name, arrow_type.value_type, nested=True)
    carried = (
        t.is_boolean(arrow_type)
        or (t.is_integer(arrow_type) and not t.is_uint64(arrow_type))
        or (t.is_floating(arrow_type) and not t.is_float16(arrow_type))
        or t.is_string(arrow_type)
        or t.is_large_string(arrow_type)
        or t.is_date(arrow_type)
        or t.is_timestamp(arrow_type)
        or t.is_time64(arrow_type)
        or t.is_null(arrow_type)
    )
    if not carried:
        raise TypeError(
            f"column '{name}' has type {arrow_type}, which graphio doesn't send as a property; "
            'cast it first (e.g. a decimal to float64)'
        )


def _normalize(pa, batch, defaults: dict | None):
    """Dictionaries decoded, types checked, `defaults` added where the batch has no column."""
    columns, names = [], []
    for name, column in zip(batch.schema.names, batch.columns, strict=True):
        decoded = _decoded_type(pa, column.type)
        if decoded != column.type:
            column = column.cast(decoded)
        _check(pa, name, column.type)
        columns.append(column)
        names.append(name)
    for key, value in (defaults or {}).items():
        if key not in names:
            columns.append(pa.array([value] * batch.num_rows))
            names.append(key)
    return pa.RecordBatch.from_arrays(columns, names=names)


def _source_batches(source, defaults: dict | None = None):
    """The source's batches, normalized (see :func:`_normalize`)."""
    pa = _pa()
    for batch in _reader(source):
        if batch.num_rows:
            yield _normalize(pa, batch, defaults)


def _endpoint_columns(keys, side):
    return [f'{side}_{key}' for key in keys]


def _relationship_batches(source, start_keys, end_keys, defaults: dict | None = None):
    """Relationship rows in graphio's query shape: ``start_<key>`` and ``end_<key>`` columns and
    a ``properties`` struct of the other columns (`defaults` filled in)."""
    pa = _pa()
    endpoints = _endpoint_columns(start_keys, 'start') + _endpoint_columns(end_keys, 'end')
    for batch in _source_batches(source):
        missing = [c for c in endpoints if c not in batch.schema.names]
        if missing:
            raise ValueError(
                f'the relationship rows have no column {", ".join(missing)} '
                f'(columns: {", ".join(batch.schema.names)})'
            )
        props = [n for n in batch.schema.names if n not in endpoints]
        names = list(props)
        children = [batch.column(n) for n in props]
        for key, value in (defaults or {}).items():
            if key not in names:
                names.append(key)
                children.append(pa.array([value] * batch.num_rows))
        if names:
            properties = pa.StructArray.from_arrays(children, names=names)
        else:
            properties = pa.array([{}] * batch.num_rows, type=pa.struct([]))
        yield pa.RecordBatch.from_arrays(
            [batch.column(c) for c in endpoints] + [properties], names=endpoints + ['properties']
        )


def _row_batches(arrow_batches, batch_size: int):
    """Dicts in lists of at most `batch_size`, for UNWIND over Bolt."""
    pending = []
    for batch in arrow_batches:
        pending.extend(batch.to_pylist())
        full = len(pending) // batch_size * batch_size
        for start in range(0, full, batch_size):
            yield pending[start : start + batch_size]
        pending = pending[full:]
    if pending:
        yield pending


def _speaks_protocol(driver) -> bool:
    """Whether the server is KwelaDB with the bulk protocol this module speaks (cached)."""
    try:
        return _servers[driver]
    except (KeyError, TypeError):
        pass
    result = False
    try:
        agent = driver.get_server_info().agent
        if '+kweladb.' not in agent:
            log.info('%s is set, but the server (%s) is not KwelaDB: graphio uses UNWIND',
                     ENV_VAR, agent)  # fmt: skip
        else:
            records, _, _ = driver.execute_query(
                'CALL kwela.protocol() YIELD version RETURN version'
            )
            version = records[0]['version']
            result = version == PROTOCOL
            if not result:
                log.warning(
                    'KwelaDB speaks bulk protocol %s, graphio %s: using UNWIND', version, PROTOCOL
                )
    except Exception as e:
        log.info("could not check KwelaDB's bulk protocol (%s): graphio uses UNWIND", e)
    try:
        _servers[driver] = result
    except TypeError:
        pass
    return result


def _upload(uri, database, name, table):
    import adbc_driver_flightsql.dbapi as flight_sql

    parts = urlsplit(uri)
    location = f'{parts.scheme}://{parts.hostname}' + (f':{parts.port}' if parts.port else '')
    db_kwargs = {}
    if parts.username is not None:
        db_kwargs['username'] = unquote(parts.username)
        db_kwargs['password'] = unquote(parts.password or '')
    if database:
        db_kwargs['adbc.flight.sql.rpc.call_header.kwela-database'] = database
    import pyarrow as pa

    with flight_sql.connect(location, db_kwargs=db_kwargs, autocommit=True) as conn:
        with conn.cursor() as cursor:
            if isinstance(table, pa.RecordBatchReader):
                reader = table  # already in batches under the limit
            else:
                reader = pa.RecordBatchReader.from_batches(table.schema, batches(table))
            cursor.adbc_ingest(name, reader, mode='replace')


def batches(table, limit: int = _BATCH_BYTES):
    """The table in record batches of at most `limit` bytes each (gRPC refuses messages over
    16 MB). Rows can differ much in size (sequences, abstracts), so each batch is measured, not
    estimated from the average row; a batch too large is halved until it fits."""
    offset, total = 0, table.num_rows
    rows = max(1, int(total * limit / max(table.nbytes, 1)))
    while offset < total:
        size = min(rows, total - offset)
        part = table.slice(offset, size)
        while part.nbytes > limit and size > 1:
            size //= 2
            part = table.slice(offset, size)
        yield from part.to_batches()
        offset += size


def _warn_once():
    global _warned
    if not _warned:
        _warned = True
        warnings.warn(
            f'graphio: bulk loads over Arrow Flight ({ENV_VAR}) are experimental',
            stacklevel=4,
        )
