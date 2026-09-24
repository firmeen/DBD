import { z } from "zod"

import { SPATIAL_LAYER_IDS } from "./types"
import type {
  SpatialLayerId,
  SpatialSurfaceSnapshot,
  SpatialViewState,
} from "./types"

export const THAILAND_VIEW: SpatialViewState = {
  longitude: 100.5018,
  latitude: 13.7563,
  zoom: 5.25,
  pitch: 0,
  bearing: 0,
}

export const DEFAULT_SPATIAL_LAYERS: SpatialLayerId[] = [
  "administrative",
  "density",
  "companies",
  "branches",
  "catchment",
  "selected",
]

const viewSchema = z.object({
  longitude: z.number().min(-180).max(180),
  latitude: z.number().min(-85).max(85),
  zoom: z.number().min(1).max(20),
  pitch: z.number().min(0).max(70),
  bearing: z.number().min(-360).max(360),
})

const surfaceSnapshotSchema = z.object({
  viewState: viewSchema,
  layers: z.array(z.enum(SPATIAL_LAYER_IDS)),
  branchId: z.string().nullable(),
  radiusKm: z.number().min(1).max(100),
  companyRailOpen: z.boolean(),
})

export function parseSpatialSurfaceSnapshot(
  input: unknown
): SpatialSurfaceSnapshot | null {
  const parsed = surfaceSnapshotSchema.safeParse(input)
  return parsed.success ? parsed.data : null
}

export function defaultSpatialSurfaceSnapshot(): SpatialSurfaceSnapshot {
  return {
    viewState: THAILAND_VIEW,
    layers: [...DEFAULT_SPATIAL_LAYERS],
    branchId: null,
    radiusKm: 15,
    companyRailOpen: true,
  }
}
