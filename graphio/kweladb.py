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
import uuid
import warnings
import weakref
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


def bulk(driver, database, query: str, parameter: str, rows: list, params: dict, lists=()) -> bool:
    """
    Run ``query``, built for ``UNWIND $<parameter> AS ...``, over ``rows`` sent with Flight.

    :param params: The query's other parameters.
    :param lists: Keys (of the rows, or of their ``properties`` maps) that the query only reads
        as ``[] + value`` (``append_props``): their scalars may be sent as one-element lists.
    :return: True if it ran; False if the caller should run it with ``UNWIND`` (the Flight
        transport is off, the server isn't KwelaDB, or the rows can't be sent exactly).
    """
    if not enabled() or len(rows) < MIN_ROWS:
        return False
    prefix = f'UNWIND ${parameter} AS '
    if not query.startswith(prefix):
        return False
    if not _speaks_protocol(driver):
        log.debug('%s is set, but the server is not KwelaDB: using UNWIND', ENV_VAR)
        return False
    try:
        import adbc_driver_flightsql  # noqa: F401
        import pyarrow  # noqa: F401
    except ImportError as e:
        raise ImportError(
            f'{ENV_VAR} needs pyarrow and adbc-driver-flightsql: pip install graphio[kweladb]'
        ) from e
    converted = to_arrow(rows, lists)
    if converted is None:
        log.debug('rows that Arrow cannot carry exactly: using UNWIND')
        return False
    table, keep_nulls = converted
    _warn_once()

    name = f'graphio_{uuid.uuid4().hex}'
    _upload(os.environ[ENV_VAR], database, name, table)
    staged = 'CALL kwela.staged($kwela_table, $kwela_options) YIELD row AS ' + query[len(prefix) :]
    options = {'keepNulls': keep_nulls}

    def run(tx):
        tx.run(staged, kwela_table=name, kwela_options=options, **params).consume()

    with driver.session(database=database) as session:
        try:
            session.execute_write(run)
        finally:
            try:
                session.run('CALL kwela.dropStaged($kwela_table)', kwela_table=name).consume()
            except Exception as e:  # the load's own error matters more
                log.warning('could not drop staged table %s: %s', name, e)
    return True


def to_arrow(rows: list, lists=()):
    """
    The rows (maps, with at most one level of nested maps) as an Arrow table, and whether its
    nulls are explicit ``None`` values (all rows have the same keys) or missing keys.

    :param lists: Keys whose scalars may become one-element lists (see :func:`bulk`).
    :return: ``(table, keep_nulls)``, or None if Arrow can't carry the rows exactly.
    """
    import pyarrow as pa

    keys = {}
    nested = {}
    first = None
    uniform = True
    has_none = False
    for row in rows:
        if not isinstance(row, dict):
            return None
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
        return None

    columns = []
    for key in keys:
        values = [row.get(key) for row in rows]
        if key in nested:
            if any(v is not None and not isinstance(v, dict) for v in values):
                return None
            if not nested[key]:
                # Maps without keys (relationships without properties).
                array = pa.array([None if v is None else {} for v in values], type=pa.struct([]))
                columns.append(array)
                continue
            children = []
            for field in nested[key]:
                column = [None if v is None else v.get(field) for v in values]
                child = _array(pa, column, field in lists)
                if child is None:
                    return None
                children.append(child)
            mask = pa.array([v is None for v in values])
            array = pa.StructArray.from_arrays(children, names=list(nested[key]), mask=mask)
        else:
            array = _array(pa, values, key in lists)
        if array is None:
            return None
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


def _array(pa, values, as_lists=False):
    """The values as one Arrow array of one exact type, or None."""
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
                kind = None if item is None else _kind(item)
                if item is not None and (kind is None or element not in (None, kind)):
                    return None
                element = element or kind
        else:
            kind = _kind(value)
            if kind is None or scalar not in (None, kind):
                return None
            scalar = kind
    if lists and scalar is not None:
        return None
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
    except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError):
        return None


def _speaks_protocol(driver) -> bool:
    """Whether the server is KwelaDB with the bulk protocol this module speaks (cached)."""
    try:
        return _servers[driver]
    except (KeyError, TypeError):
        pass
    result = False
    try:
        if '+kweladb.' in driver.get_server_info().agent:
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
        log.debug('KwelaDB protocol check failed: %s', e)
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
    rows_per_batch = max(1, int(table.num_rows * _BATCH_BYTES / max(table.nbytes, 1)))
    with flight_sql.connect(location, db_kwargs=db_kwargs, autocommit=True) as conn:
        with conn.cursor() as cursor:
            reader = table.to_reader(max_chunksize=rows_per_batch)
            cursor.adbc_ingest(name, reader, mode='replace')


def _warn_once():
    global _warned
    if not _warned:
        _warned = True
        warnings.warn(
            f'graphio: bulk loads over Arrow Flight ({ENV_VAR}) are experimental',
            stacklevel=4,
        )
