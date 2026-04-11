import { useQuery, useMutation } from "@tanstack/react-query";
import { get, post, del } from "./client";
import type {
  OverviewResponse,
  TypesResponse,
  SchemaResponse,
  MetadataResponse,
  BrowseResponse,
  TableSource,
  SqlResponse,
  SubLogListResponse,
  DfgResponse,
  VariantsResponse,
} from "./types";

// --- Overview & Metadata ---

export function useOverview() {
  return useQuery({
    queryKey: ["overview"],
    queryFn: () => get<OverviewResponse>("/api/overview"),
  });
}

export function useTypes() {
  return useQuery({
    queryKey: ["types"],
    queryFn: () => get<TypesResponse>("/api/types"),
  });
}

export function useSchema(table: TableSource) {
  return useQuery({
    queryKey: ["schema", table],
    queryFn: () => get<SchemaResponse>(`/api/schema/${table}`),
  });
}

export function useMetadata() {
  return useQuery({
    queryKey: ["metadata"],
    queryFn: () => get<MetadataResponse>("/api/metadata"),
  });
}

// --- Browse ---

export function useBrowse(source: TableSource, limit: number, offset: number) {
  return useQuery({
    queryKey: ["browse", source, limit, offset],
    queryFn: () =>
      get<BrowseResponse>(`/api/browse/${source}`, { limit, offset }),
  });
}

// --- SQL ---

export function useSqlExecute() {
  return useMutation({
    mutationFn: ({ query, limit }: { query: string; limit: number }) =>
      post<SqlResponse>("/api/sql/execute", { query, limit }),
  });
}

// --- Sublogs ---

export function useSublogs() {
  return useQuery({
    queryKey: ["sublogs"],
    queryFn: () => get<SubLogListResponse>("/api/sublogs"),
  });
}

export function useDeleteSublog() {
  return useMutation({
    mutationFn: (id: string) => del(`/api/sublogs/${id}`),
  });
}

// --- Process Map ---

export function useDfg(objectType: string) {
  return useQuery({
    queryKey: ["dfg", objectType],
    queryFn: () => get<DfgResponse>(`/api/process-map/dfg/${objectType}`),
    enabled: !!objectType,
  });
}

export function useVariants(objectType: string) {
  return useQuery({
    queryKey: ["variants", objectType],
    queryFn: () => get<VariantsResponse>(`/api/process-map/variants/${objectType}`),
    enabled: !!objectType,
  });
}
