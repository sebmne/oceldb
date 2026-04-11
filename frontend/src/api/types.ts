// --- Overview ---

export type OverviewStats = {
  event_count: number;
  object_count: number;
  event_type_count: number;
  object_type_count: number;
  earliest_event_time: string | null;
  latest_event_time: string | null;
};

export type EventObjectStats = {
  avg_objects_per_event: number | null;
  min_objects_per_event: number | null;
  max_objects_per_event: number | null;
  avg_events_per_object: number | null;
  min_events_per_object: number | null;
  max_events_per_object: number | null;
};

export type OverviewResponse = {
  overview: OverviewStats;
  event_type_counts: Record<string, number>;
  object_type_counts: Record<string, number>;
  event_object_stats: EventObjectStats;
};

export type TypesResponse = {
  event_types: string[];
  object_types: string[];
};

export type AttributesResponse = {
  attributes: string[];
};

export type SchemaColumn = {
  name: string;
  type: string;
};

export type SchemaResponse = {
  columns: SchemaColumn[];
};

export type MetadataResponse = {
  source: string;
  oceldb_version: string;
  converted_at: string | null;
};

// --- Browse ---

export type BrowseResponse = {
  columns: string[];
  rows: unknown[][];
  total_count: number;
};

export type TableSource = "event" | "object" | "event_object" | "object_object";

// --- View Spec ---

export type CastName = "int" | "float" | "str" | "bool" | "datetime" | null;
export type ViewRoot = "event" | "object";

export type FieldRef = { kind: "field"; name: string; cast: CastName };
export type AttrRef = { kind: "attr"; name: string; cast: CastName };
export type ExprRef = FieldRef | AttrRef;

export type ComparisonFilter = {
  kind: "comparison";
  left: ExprRef;
  op: "==" | "!=" | ">" | ">=" | "<" | "<=";
  right: unknown;
};

export type NullFilter = {
  kind: "null_check";
  expr: ExprRef;
  op: "is_null" | "is_not_null";
};

export type RelatedExistsFilter = {
  kind: "related_exists";
  object_type: string;
};

export type LinkedExistsFilter = {
  kind: "linked_exists";
  object_type: string;
};

export type HasEventExistsFilter = {
  kind: "has_event_exists";
  event_type: string;
};

export type ViewFilter =
  | ComparisonFilter
  | NullFilter
  | RelatedExistsFilter
  | LinkedExistsFilter
  | HasEventExistsFilter;

export type ViewSpec = {
  root: ViewRoot;
  types: string[];
  filters: ViewFilter[];
};

export type ViewPreviewResponse = {
  count: number;
  sql: string;
  columns: string[];
  rows: unknown[][];
};

// --- Table Spec ---

export type AggKind = "count" | "count_distinct" | "min" | "max" | "sum" | "avg";
export type SortDirection = "ASC" | "DESC";

export type SourceField = { kind: "field"; name: string; cast: CastName };

export type SelectItem = { expr: SourceField; alias: string | null };
export type GroupByItem = { expr: SourceField };
export type AggItem = { kind: AggKind; expr: SourceField | null; alias: string | null };
export type OrderByItem = { by: string | SourceField; direction: SortDirection };

export type TableSpec = {
  source: TableSource;
  select: SelectItem[];
  group_by: GroupByItem[];
  agg: AggItem[];
  order_by: OrderByItem[];
  distinct: boolean;
  limit: number | null;
};

export type TablePreviewResponse = {
  sql: string;
  columns: string[];
  rows: unknown[][];
  row_count: number;
};

// --- SQL ---

export type SqlResponse = {
  columns: string[];
  rows: unknown[][];
  row_count: number;
  execution_time_ms: number;
};

// --- Sublogs ---

export type SubLogEntry = {
  id: string;
  root: string;
  types: string[];
  filter_count: number;
};

export type SubLogListResponse = {
  sublogs: SubLogEntry[];
};

// --- Process Map ---

export type DfgNode = {
  id: string;
  label: string;
  frequency: number;
  is_start: boolean;
  is_end: boolean;
  start_count: number;
  end_count: number;
};

export type DfgEdge = {
  source: string;
  target: string;
  frequency: number;
};

export type DfgResponse = {
  nodes: DfgNode[];
  edges: DfgEdge[];
};

export type VariantEntry = {
  id: number;
  activities: string[];
  frequency: number;
  percentage: number;
};

export type VariantsResponse = {
  variants: VariantEntry[];
};
