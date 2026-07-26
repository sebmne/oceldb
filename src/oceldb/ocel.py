"""Public OCEL handle and lazy table-access API."""

from collections.abc import Callable
from pathlib import Path
from typing import TypeVar
import polars as pl

from oceldb.core import schema as s
from oceldb.core.sql import execute_sql
from oceldb.errors import OCELValidationError
from oceldb.types import FrameLike, OneOrMany, PathLikeStr, normalize_strings

_T = TypeVar("_T")


class OCEL:
    """Represent an OCEL 2.0 log as five lazy Polars tables.

    An ``OCEL`` is a lightweight, immutable handle. Accessing a table creates
    a lazy query plan; it does not load rows into memory. Use :meth:`open` to
    read a native snapshot and :meth:`from_frames` to construct a log from
    Polars frames.

    Native snapshots retain partition metadata. Consequently, type-filtered
    event and object-change queries can prune files and omit attribute columns
    that do not belong to the requested types. Relation access automatically
    selects the event- or object-oriented physical representation.

    Attributes:
        source: Absolute path of the native snapshot, or ``None`` when the log
            was constructed in memory.
        format_version: Native storage-format version, or ``None`` when the
            log was constructed in memory.

    Note:
        The low-level constructor does not validate its inputs and is intended
        for internal use. Application code should use :meth:`open`,
        :meth:`from_frames`, or :meth:`empty`.

    """

    __slots__ = (
        "_events",
        "_objects",
        "_object_changes",
        "_e2o",
        "_e2o_by_object",
        "_e2o_by_event_sorted",
        "_o2o",
        "_event_attributes",
        "_change_attributes",
        "_event_types",
        "_object_types",
        "_source",
        "_format_version",
    )

    def __init__(
        self,
        *,
        events: pl.LazyFrame,
        objects: pl.LazyFrame,
        object_changes: pl.LazyFrame,
        e2o: pl.LazyFrame,
        o2o: pl.LazyFrame,
        _e2o_by_object: pl.LazyFrame | None = None,
        _e2o_by_event_sorted: bool = False,
        event_attributes: dict[str, list[str]] | None = None,
        change_attributes: dict[str, list[str]] | None = None,
        _event_types: tuple[str, ...] | None = None,
        _object_types: tuple[str, ...] | None = None,
        _source: Path | None = None,
        _format_version: int | None = None,
    ) -> None:
        """Initialize an OCEL from trusted lazy frames.

        This low-level constructor performs no schema or integrity validation.
        Use :meth:`from_frames` at public construction boundaries.

        Args:
            events: Event rows in the canonical event schema.
            objects: Object identities in the canonical object schema.
            object_changes: Initial object states and attribute changes in the
                canonical object-change schema.
            e2o: Event-to-object relations in the canonical E2O schema.
            o2o: Object-to-object relations in the canonical O2O schema.
            _e2o_by_object: Optional native E2O index sorted by object and
                event identifier.
            _e2o_by_event_sorted: Whether canonical E2O rows carry the
                declared event-oriented physical order.
            event_attributes: Event attribute columns by event type. ``None``
                indicates that column-narrowing metadata is unavailable.
            change_attributes: Object attribute columns by object type.
                ``None`` indicates that column-narrowing metadata is
                unavailable.
            _event_types: Event types obtained from native partition metadata.
            _object_types: Object types obtained from native partition
                metadata.
            _source: Absolute native snapshot path.
            _format_version: Native storage-format version.

        """
        self._events = events
        self._objects = objects
        self._object_changes = object_changes
        self._e2o = e2o
        self._e2o_by_object = _e2o_by_object
        self._e2o_by_event_sorted = _e2o_by_event_sorted
        self._o2o = o2o
        # ``open()`` derives these maps from partition Parquet footers.
        # ``None`` means narrowing is unknown, so typed accessors fall back to
        # plain type filters.
        self._event_attributes = event_attributes
        self._change_attributes = change_attributes
        self._event_types = _event_types
        self._object_types = _object_types
        self._source = _source
        self._format_version = _format_version

    @property
    def source(self) -> Path | None:
        """Return the native snapshot path.

        Returns:
            The absolute snapshot path, or ``None`` for an in-memory log.

        """
        return self._source

    @property
    def format_version(self) -> int | None:
        """Return the native storage-format version.

        Returns:
            The format version, or ``None`` for an in-memory log.

        """
        return self._format_version

    def __repr__(self) -> str:
        """Return a concise representation without evaluating table data."""
        source = None if self._source is None else str(self._source)
        return (
            f"{type(self).__name__}(source={source!r}, "
            f"format_version={self._format_version!r})"
        )

    @classmethod
    def from_frames(
        cls,
        *,
        events: FrameLike,
        objects: FrameLike,
        object_changes: FrameLike,
        e2o: FrameLike,
        o2o: FrameLike | None = None,
        validate: bool = False,
    ) -> "OCEL":
        """Create an OCEL from eager or lazy Polars frames.

        All inputs are normalized to :class:`polars.LazyFrame`. Canonical
        column names and dtypes are validated immediately without evaluating
        row data. Set ``validate=True`` to additionally execute the complete
        logical integrity checks.

        Args:
            events: Event table. Required columns are ``ocel_id`` (string),
                ``ocel_time`` (microsecond UTC datetime), and ``ocel_type``
                (string). Additional columns are event attributes.
            objects: Object identity table. Required columns are ``ocel_id``
                and ``ocel_type``, both strings. Additional columns are not
                permitted.
            object_changes: Object-change table. Required columns are
                ``ocel_id``, ``ocel_time``, ``ocel_changed_field``,
                ``ocel_is_initial``, and ``ocel_type``. Additional columns are
                object attributes.
            e2o: Event-to-object relation table in the canonical E2O schema.
            o2o: Object-to-object relation table in the canonical O2O schema.
                If omitted, an empty table with the canonical schema is used.
            validate: Whether to execute row-level logical validation.
                Defaults to ``False``.

        Returns:
            A new OCEL backed by lazy frames.

        Raises:
            OCELValidationError: If a table has an invalid schema, or if
                ``validate=True`` and a logical invariant is violated.

        """
        frames = {
            "events": cls._as_lazy(events),
            "objects": cls._as_lazy(objects),
            "object_changes": cls._as_lazy(object_changes),
            "e2o": cls._as_lazy(e2o),
            "o2o": (
                pl.LazyFrame(schema=s.O2O_SCHEMA) if o2o is None else cls._as_lazy(o2o)
            ),
        }
        from oceldb.core.validation import validate_schemas

        validate_schemas(**frames)
        result = cls(
            events=frames["events"],
            objects=frames["objects"],
            object_changes=frames["object_changes"],
            e2o=frames["e2o"],
            o2o=frames["o2o"],
        )
        if validate:
            result.validate()
        return result

    @classmethod
    def empty(cls) -> "OCEL":
        """Create an empty OCEL with canonical table schemas.

        Returns:
            A valid OCEL whose five tables contain no rows.

        """
        return cls.from_frames(
            events=pl.LazyFrame(schema=s.EVENT_SCHEMA),
            objects=pl.LazyFrame(schema=s.OBJECT_SCHEMA),
            object_changes=pl.LazyFrame(schema=s.CHANGE_SCHEMA),
            e2o=pl.LazyFrame(schema=s.E2O_SCHEMA),
        )

    def append(
        self,
        *,
        events: FrameLike | None = None,
        objects: FrameLike | None = None,
        object_changes: FrameLike | None = None,
        e2o: FrameLike | None = None,
        o2o: FrameLike | None = None,
        validate: bool = False,
    ) -> "OCEL":
        """Append rows to one or more tables and return a new OCEL.

        The operation is immutable and lazy: neither this OCEL nor the input
        frames are modified or evaluated. Event and object-change schemas are
        combined by column name, allowing new attribute columns. The object,
        E2O, and O2O tables require their exact canonical schemas.

        Appending preserves every supplied row and does not deduplicate
        identifiers or relations. Set ``validate=True`` to verify the complete
        logical OCEL after concatenation.

        Args:
            events: Event rows to append.
            objects: Object identity rows to append.
            object_changes: Initial object states and object-change rows to
                append.
            e2o: Event-to-object relations to append.
            o2o: Object-to-object relations to append.
            validate: Whether to execute row-level logical validation on the
                resulting OCEL. Defaults to ``False``.

        Returns:
            A new OCEL containing the existing and appended rows.

        Raises:
            ValueError: If no table is provided.
            OCELValidationError: If an existing or appended table has an
                invalid schema, if shared attributes have incompatible dtypes,
                or if ``validate=True`` and a logical invariant is violated.

        """
        additions = {
            "events": None if events is None else self._as_lazy(events),
            "objects": None if objects is None else self._as_lazy(objects),
            "object_changes": (
                None if object_changes is None else self._as_lazy(object_changes)
            ),
            "e2o": None if e2o is None else self._as_lazy(e2o),
            "o2o": None if o2o is None else self._as_lazy(o2o),
        }
        provided = {
            name: frame for name, frame in additions.items() if frame is not None
        }
        if not provided:
            raise ValueError("OCEL.append() requires at least one table.")

        from oceldb.core.validation import validate_table_schemas

        validate_table_schemas(
            {
                "events": self._events,
                "objects": self._objects,
                "object_changes": self._object_changes,
                "e2o": self._e2o,
                "o2o": self._o2o,
            }
        )
        validate_table_schemas(provided)

        return OCEL.from_frames(
            events=self._concat_table(
                self._events,
                additions["events"],
                allow_attributes=True,
            ),
            objects=self._concat_table(
                self._objects,
                additions["objects"],
                schema=s.OBJECT_SCHEMA,
            ),
            object_changes=self._concat_table(
                self._object_changes,
                additions["object_changes"],
                allow_attributes=True,
            ),
            e2o=self._concat_table(
                self._e2o,
                additions["e2o"],
                schema=s.E2O_SCHEMA,
            ),
            o2o=self._concat_table(
                self._o2o,
                additions["o2o"],
                schema=s.O2O_SCHEMA,
            ),
            validate=validate,
        )

    def events(self, *types: str, ids: OneOrMany[str] | None = None) -> pl.LazyFrame:
        """Build a lazy query over events.

        Type and identifier filters are combined with logical AND. Omitting a
        filter leaves that dimension unrestricted. Passing an empty identifier
        iterable produces an empty result.

        Args:
            *types: Event types to include. If omitted, include every event
                type.
            ids: One event identifier or an iterable of identifiers to
                include.

        Returns:
            A lazy frame containing the canonical event columns and event
            attributes. For native snapshots, specifying ``types`` also
            narrows the attribute columns to those stored for the requested
            types.

        Raises:
            TypeError: If a selector is not a string or iterable of strings.
            ValueError: If a selector contains an empty string.

        """
        selected = self._typed(
            self._events,
            types,
            s.EVENT_SCHEMA,
            self._event_attributes,
        )
        return self._filter_identifier(selected, s.OCEL_ID, ids)

    def objects(self, *types: str, ids: OneOrMany[str] | None = None) -> pl.LazyFrame:
        """Build a lazy query over object identities.

        Type and identifier filters are combined with logical AND. Omitting a
        filter leaves that dimension unrestricted. Passing an empty identifier
        iterable produces an empty result.

        Args:
            *types: Object types to include. If omitted, include every object
                type.
            ids: One object identifier or an iterable of identifiers to
                include.

        Returns:
            A lazy frame containing ``ocel_id`` and ``ocel_type``. Object
            attributes are represented by :meth:`object_changes`, not by the
            identity table.

        Raises:
            TypeError: If a selector is not a string or iterable of strings.
            ValueError: If a selector contains an empty string.

        """
        selected = self._typed(self._objects, types, s.OBJECT_SCHEMA, None)
        return self._filter_identifier(selected, s.OCEL_ID, ids)

    def object_changes(
        self, *types: str, ids: OneOrMany[str] | None = None
    ) -> pl.LazyFrame:
        """Build a lazy query over object states and attribute changes.

        The returned rows are the stored initial states and changes; they are
        not a materialized point-in-time snapshot. Type and identifier filters
        are combined with logical AND.

        Args:
            *types: Object types to include. If omitted, include every object
                type.
            ids: One object identifier or an iterable of identifiers to
                include.

        Returns:
            A lazy frame containing the canonical object-change columns and
            object attributes. For native snapshots, specifying ``types`` also
            narrows the attribute columns to those stored for the requested
            types.

        Raises:
            TypeError: If a selector is not a string or iterable of strings.
            ValueError: If a selector contains an empty string.

        """
        selected = self._typed(
            self._object_changes,
            types,
            s.CHANGE_SCHEMA,
            self._change_attributes,
        )
        return self._filter_identifier(selected, s.OCEL_ID, ids)

    def object_states(
        self, *types: str, ids: OneOrMany[str] | None = None
    ) -> pl.LazyFrame:
        """Build a lazy history of complete object attribute states.

        Each row represents an object's complete known state at one recorded
        change timestamp. Sparse attribute changes are forward-filled per
        object. Type and identifier filters are applied before reconstruction.

        Unlike :meth:`object_changes`, this derived accessor retains the union
        of object attribute columns so its schema is identical for equivalent
        in-memory and native logs.

        Args:
            *types: Object types to include. If omitted, include every object
                type.
            ids: One object identifier or an iterable of identifiers to
                include.

        Returns:
            A lazy frame containing ``ocel_id``, ``ocel_time``, the complete
            object attribute state, and ``ocel_type``. Rows are sorted by
            object type, object identifier, and timestamp.

        Raises:
            TypeError: If a selector is neither a string nor an iterable of
                strings.
            ValueError: If a selector contains an empty string.

        """
        from oceldb.core.states import reconstruct_object_states

        changes = self._filter_relation(
            self._object_changes,
            (
                (s.OCEL_TYPE, types or None),
                (s.OCEL_ID, ids),
            ),
        )
        return reconstruct_object_states(changes)

    @property
    def event_types(self) -> list[str]:
        """Return the event types present in the log.

        Native snapshots use partition metadata and do not read table rows.
        Other logs execute a streaming distinct query over ``ocel_type``.

        Returns:
            Event type names in ascending lexical order.

        Raises:
            OCELValidationError: If a queried type name is null or empty.

        """
        if self._event_types is not None:
            return list(self._event_types)
        return self._distinct_types(self._events)

    @property
    def object_types(self) -> list[str]:
        """Return the object types present in the log.

        Native snapshots use partition metadata and do not read table rows.
        Other logs execute a streaming distinct query over ``ocel_type``.

        Returns:
            Object type names in ascending lexical order.

        Raises:
            OCELValidationError: If a queried type name is null or empty.

        """
        if self._object_types is not None:
            return list(self._object_types)
        return self._distinct_types(self._objects)

    def e2o(
        self,
        *,
        event_types: OneOrMany[str] | None = None,
        object_types: OneOrMany[str] | None = None,
        event: OneOrMany[str] | None = None,
        object: OneOrMany[str] | None = None,
        qualifier: OneOrMany[str] | None = None,
    ) -> pl.LazyFrame:
        """Build a lazy query over event-to-object relations.

        Every supplied filter is applied with logical AND. A selector may be a
        single string or an iterable of strings. ``None`` leaves the
        corresponding column unrestricted; an empty iterable produces an empty
        result.

        Args:
            event_types: Event type or event types to include.
            object_types: Object type or object types to include.
            event: Event identifier or identifiers to include.
            object: Object identifier or identifiers to include.
            qualifier: Relation qualifier or qualifiers to include.

        Returns:
            A lazy frame in the canonical E2O schema containing only matching
            relations. Native snapshots choose the object-oriented physical
            index when only object-side selectors are supplied; logical row
            contents and ordering guarantees are unchanged.

        Raises:
            TypeError: If a selector is neither a string nor an iterable of
                strings.
            ValueError: If a selector contains an empty string.

        """
        frame = self._e2o
        if (
            self._e2o_by_object is not None
            and event_types is None
            and event is None
            and (object_types is not None or object is not None)
        ):
            frame = self._e2o_by_object
        return self._filter_relation(
            frame,
            (
                (s.OCEL_EVENT_TYPE, event_types),
                (s.OCEL_OBJECT_TYPE, object_types),
                (s.OCEL_EVENT_ID, event),
                (s.OCEL_OBJECT_ID, object),
                (s.OCEL_QUALIFIER, qualifier),
            ),
        )

    def o2o(
        self,
        *,
        source_types: OneOrMany[str] | None = None,
        target_types: OneOrMany[str] | None = None,
        source: OneOrMany[str] | None = None,
        target: OneOrMany[str] | None = None,
        qualifier: OneOrMany[str] | None = None,
    ) -> pl.LazyFrame:
        """Build a lazy query over object-to-object relations.

        Every supplied filter is applied with logical AND. A selector may be a
        single string or an iterable of strings. ``None`` leaves the
        corresponding column unrestricted; an empty iterable produces an empty
        result.

        Args:
            source_types: Source object type or types to include.
            target_types: Target object type or types to include.
            source: Source object identifier or identifiers to include.
            target: Target object identifier or identifiers to include.
            qualifier: Relation qualifier or qualifiers to include.

        Returns:
            A lazy frame in the canonical O2O schema containing only matching
            relations.

        Raises:
            TypeError: If a selector is neither a string nor an iterable of
                strings.
            ValueError: If a selector contains an empty string.

        """
        return self._filter_relation(
            self._o2o,
            (
                (s.OCEL_SOURCE_TYPE, source_types),
                (s.OCEL_TARGET_TYPE, target_types),
                (s.OCEL_SOURCE_ID, source),
                (s.OCEL_TARGET_ID, target),
                (s.OCEL_QUALIFIER, qualifier),
            ),
        )

    def sql(self, query: str) -> pl.DataFrame:
        """Execute a DuckDB query over the five OCEL tables.

        The method registers ``events``, ``objects``, ``object_changes``,
        ``e2o``, and ``o2o`` as DuckDB views. Unlike the table accessors, SQL
        execution is eager and materializes the query result.

        Args:
            query: SQL statement that references any of the registered table
                names.

        Returns:
            The materialized query result as a Polars data frame.

        Raises:
            ModuleNotFoundError: If the optional SQL dependencies are absent.
                Install them with ``pip install "oceldb[sql]"``.
            duckdb.Error: If DuckDB cannot bind or execute the query.

        Example:
            Count events by type::

                result = ocel.sql(
                    "SELECT ocel_type, count(*) AS count "
                    "FROM events GROUP BY ocel_type"
                )

        """
        return execute_sql(
            query,
            events=self._events,
            objects=self._objects,
            object_changes=self._object_changes,
            e2o=self._e2o,
            o2o=self._o2o,
        )

    @classmethod
    def open(cls, path: PathLikeStr, *, validate: bool = False) -> "OCEL":
        """Open a native oceldb snapshot.

        Opening validates the manifest, directory layout, and Parquet schemas
        by reading metadata only. Row data remains lazy unless
        ``validate=True``, in which case logical validation evaluates the
        tables.

        Args:
            path: Path to the native snapshot directory.
            validate: Whether to execute row-level logical validation after
                opening. Defaults to ``False``.

        Returns:
            An OCEL backed by lazy Parquet scans.

        Raises:
            FileNotFoundError: If ``path``, its manifest, or a required table
                is missing.
            OCELFormatError: If the snapshot has an unsupported or malformed
                manifest, layout, or physical schema.
            OCELValidationError: If ``validate=True`` and the logical tables
                violate the OCEL contract.

        """
        from oceldb.core.open import open_native

        result = open_native(path)
        if validate:
            result.validate()
        return result

    def write(
        self,
        target: PathLikeStr,
        *,
        overwrite: bool = False,
        validate: bool = False,
    ) -> None:
        """Write the log as a native oceldb snapshot.

        Writing evaluates the five lazy tables. An unchanged native handle is
        copied directly without decoding and re-encoding Parquet data. The
        complete snapshot is staged beside ``target`` and physically validated
        before installation.
        Existing paths are never replaced unless ``overwrite=True``; even
        then, only a valid native snapshot may be replaced.

        Args:
            target: Destination directory for the snapshot.
            overwrite: If ``True``, replace an existing valid native dataset
                at ``target``. Unrelated paths and final symlinks are never
                replaced. Defaults to ``False``.
            validate: Whether to execute logical validation before writing.
                Defaults to ``False``.

        Raises:
            FileExistsError: If ``target`` exists and ``overwrite`` is
                ``False``.
            OCELStorageError: If the destination cannot be installed or
                replaced safely.
            OCELValidationError: If ``validate=True`` and a logical invariant
                is violated.
            ValueError: If a trusted low-level frame has an invalid schema.

        """
        from oceldb.core.write import copy_native, write_native

        if validate:
            self.validate()
        if self._source is not None:
            copy_native(self._source, target, overwrite=overwrite)
            return
        write_native(
            target,
            events=self._events,
            objects=self._objects,
            object_changes=self._object_changes,
            e2o=self._e2o,
            o2o=self._o2o,
            overwrite=overwrite,
        )

    def validate(self) -> None:
        """Validate the complete logical OCEL contract.

        Validation evaluates the lazy tables. It checks canonical schemas,
        required values, identifier uniqueness, endpoint references,
        denormalized relation types, and object-change invariants. The log is
        not modified.

        Raises:
            OCELValidationError: If one or more invariants are violated.

        """
        from oceldb.core.validation import validate_tables

        validate_tables(
            events=self._events,
            objects=self._objects,
            object_changes=self._object_changes,
            e2o=self._e2o,
            o2o=self._o2o,
        )

    def __rshift__(self, step: Callable[["OCEL"], _T]) -> _T:
        """Pass this OCEL to a callable.

        ``ocel >> step`` is equivalent to ``step(ocel)``.

        Args:
            step: Callable that accepts this OCEL.

        Returns:
            The callable's return value.

        """
        return step(self)

    def _derive(
        self,
        *,
        events: pl.LazyFrame | None = None,
        objects: pl.LazyFrame | None = None,
        object_changes: pl.LazyFrame | None = None,
        e2o: pl.LazyFrame | None = None,
        o2o: pl.LazyFrame | None = None,
    ) -> "OCEL":
        """Create a trusted row-filtered derivative of this handle."""
        return OCEL(
            events=self._events if events is None else events,
            objects=self._objects if objects is None else objects,
            object_changes=(
                self._object_changes if object_changes is None else object_changes
            ),
            e2o=self._e2o if e2o is None else e2o,
            _e2o_by_object=self._e2o_by_object if e2o is None else None,
            _e2o_by_event_sorted=(self._e2o_by_event_sorted if e2o is None else False),
            o2o=self._o2o if o2o is None else o2o,
            event_attributes=self._event_attributes,
            change_attributes=self._change_attributes,
        )

    def _e2o_object_oriented(self) -> tuple[pl.LazyFrame, bool]:
        """Return an object-oriented E2O scan and its sortedness."""
        if self._e2o_by_object is not None:
            return self._e2o_by_object, True
        return self._e2o, False

    def _e2o_event_oriented(self) -> tuple[pl.LazyFrame, bool]:
        """Return an event-oriented E2O scan and its sortedness."""
        return self._e2o, self._e2o_by_event_sorted

    def _object_attribute_names(self, object_type: str) -> list[str]:
        """Return attributes carrying values for one object type."""
        if self._change_attributes is not None:
            return list(self._change_attributes.get(object_type, ()))

        attributes = [
            name
            for name in self._object_changes.collect_schema().names()
            if name not in s.CHANGE_SCHEMA
        ]
        if not attributes:
            return []
        presence = (
            self._object_changes.filter(pl.col(s.OCEL_TYPE) == object_type)
            .select(
                pl.col(attribute).is_not_null().any().alias(attribute)
                for attribute in attributes
            )
            .collect(engine="streaming")
        )
        if presence.is_empty():
            return []
        row = presence.row(0, named=True)
        return [attribute for attribute in attributes if row[attribute]]

    @staticmethod
    def _typed(
        frame: pl.LazyFrame,
        types: tuple[str, ...],
        schema: dict[str, pl.DataType],
        attributes: dict[str, list[str]] | None,
    ) -> pl.LazyFrame:
        """Apply a type filter and, when known, narrow attribute columns.

        Args:
            frame: Table to filter.
            types: Type names to include.
            schema: Canonical schema whose columns must remain in the result.
            attributes: Attribute columns by type, or ``None`` when unknown.

        Returns:
            The original lazy frame when ``types`` is empty; otherwise, a
            filtered lazy frame.

        """
        normalized = normalize_strings(types, name="types")
        if not normalized:
            return frame
        selected = frame.filter(pl.col(s.OCEL_TYPE).is_in(normalized))
        if attributes is None:
            return selected
        core = (name for name in schema if name != s.OCEL_TYPE)
        kept = list(
            dict.fromkeys(
                name
                for type_name in normalized
                for name in attributes.get(type_name, ())
            )
        )
        return selected.select(*core, *kept, s.OCEL_TYPE)

    @staticmethod
    def _filter_relation(
        frame: pl.LazyFrame,
        filters: tuple[tuple[str, OneOrMany[str] | None], ...],
    ) -> pl.LazyFrame:
        """Apply scalar-or-membership predicates without evaluating rows."""
        result = frame
        for column, value in filters:
            values = normalize_strings(value, name=f"{column} filter")
            if values is None:
                continue
            predicate = (
                pl.col(column) == values[0]
                if len(values) == 1
                else pl.col(column).is_in(values)
            )
            result = result.filter(predicate)
        return result

    @staticmethod
    def _filter_identifier(
        frame: pl.LazyFrame,
        column: str,
        values: OneOrMany[str] | None,
    ) -> pl.LazyFrame:
        """Apply an identifier selector without evaluating rows."""
        return OCEL._filter_relation(frame, ((column, values),))

    @staticmethod
    def _distinct_types(frame: pl.LazyFrame) -> list[str]:
        """Collect distinct, non-empty type names using streaming execution."""
        values = (
            frame.select(s.OCEL_TYPE)
            .unique()
            .sort(s.OCEL_TYPE)
            .collect(engine="streaming")
            .get_column(s.OCEL_TYPE)
            .to_list()
        )
        if not all(isinstance(value, str) and value for value in values):
            raise OCELValidationError(
                "Invalid OCEL: type names must be non-null, non-empty strings."
            )
        return values

    @staticmethod
    def _concat_table(
        current: pl.LazyFrame,
        addition: pl.LazyFrame | None,
        *,
        allow_attributes: bool = False,
        schema: dict[str, pl.DataType] | None = None,
    ) -> pl.LazyFrame:
        """Concatenate a validated table addition without evaluating rows."""
        if addition is None:
            return current
        if allow_attributes:
            return pl.concat((current, addition), how="diagonal")
        assert schema is not None
        columns = tuple(schema)
        return pl.concat(
            (current.select(*columns), addition.select(*columns)),
            how="vertical",
        )

    @staticmethod
    def _as_lazy(frame: FrameLike) -> pl.LazyFrame:
        """Convert an eager frame to lazy form without evaluating a lazy input."""
        if isinstance(frame, pl.DataFrame):
            return frame.lazy()
        return frame
