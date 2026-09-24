import type {
  EntityRef,
  LensState,
  PopulationRef,
} from "@/lib/intelligence/types"

export const SPATIAL_LAYER_IDS = [
  "administrative",
  "density",
  "opportunity",
  "companies",
  "branches",
  "catchment",
  "selected",
] as const

export type SpatialLayerId = (typeof SPATIAL_LAYER_IDS)[number]

export type SpatialLod = "national" | "regional" | "local" | "entity"

export type SpatialAvailability = "ready" | "partial" | "unavailable"

export type SpatialBounds = [
  west: number,
  south: number,
  east: number,
  north: number,
]

export const MAX_SPATIAL_COMPANY_POINTS = 25_000
export const MAX_SPATIAL_EXACT_COMPANY_IDS = 500

export interface SpatialViewState {
  longitude: number
  latitude: number
  zoom: number
  pitch: number
  bearing: number
}

export interface SpatialCapabilities {
  administrativeGeometry: boolean
  preciseCompanyGeometry: boolean
  branches: boolean
  h3Aggregates: boolean
  opportunityOverlay: boolean
}

export interface SpatialCompanyPoint {
  id: string
  name: string
  coordinates: [longitude: number, latitude: number]
  businessType?: string | null
  province?: string | null
  district?: string | null
  revenue?: number | null
  tipStatus?: "active" | "expired" | "prospect" | null
  boardCount?: number | null
  logoUrl?: string | null
}

export interface SpatialBranch {
  id: string
  name: string
  coordinates: [longitude: number, latitude: number]
  address?: string | null
}

export interface SpatialH3Cell {
  index: string
  count: number
  active: number
  expired: number
  prospect: number
  opportunityScore?: number | null
}

export interface SpatialArea {
  id: string
  name: string
  level: "province" | "district"
  geometry:
    | {
        type: "Polygon"
        coordinates: number[][][]
      }
    | {
        type: "MultiPolygon"
        coordinates: number[][][][]
      }
  companyCount?: number | null
  tipPenetration?: number | null
}

export interface SpatialTotals {
  companies: number | null
  active: number | null
  expired: number | null
  prospect: number | null
}

export interface SpatialSnapshot {
  availability: SpatialAvailability
  message?: string
  generatedAt?: string | null
  capabilities: SpatialCapabilities
  totals: SpatialTotals
  companies: SpatialCompanyPoint[]
  branches: SpatialBranch[]
  h3Cells: SpatialH3Cell[]
  areas: SpatialArea[]
}

export interface SpatialQuery {
  viewport: SpatialViewState
  bounds: SpatialBounds | null
  lod: SpatialLod
  h3Resolution: number
  lens: LensState
  population: PopulationRef | null
  exactCompanyIds: string[]
  layers: SpatialLayerId[]
  branchId: string | null
  radiusKm: number
}

export interface SpatialSurfaceSnapshot {
  viewState: SpatialViewState
  layers: SpatialLayerId[]
  branchId: string | null
  radiusKm: number
  companyRailOpen: boolean
}

export function companyToEntityRef(company: SpatialCompanyPoint): EntityRef {
  return {
    id: company.id,
    kind: "company",
    label: company.name,
    secondary:
      [company.businessType, company.province].filter(Boolean).join(" · ") ||
      undefined,
  }
}

export function branchToEntityRef(branch: SpatialBranch): EntityRef {
  return {
    id: branch.id,
    kind: "branch",
    label: branch.name,
    secondary: branch.address ?? undefined,
  }
}
