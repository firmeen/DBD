export const SURFACE_IDS = ["M", "C", "P", "R", "A", "O"] as const

export type SurfaceId = (typeof SURFACE_IDS)[number]

export type EntityKind = "company" | "person" | "area" | "branch"

export interface EntityRef {
  id: string
  kind: EntityKind
  label: string
  secondary?: string
}

export interface PopulationRef {
  key: string
  label: string
  count: number
  source: "analytics" | "spatial" | "relationship"
  exact: boolean
  expiresAt?: string | null
}

export interface SurfaceSnapshot {
  updatedAt: number
  payload: Record<string, unknown>
}

export interface LensState {
  province: string | null
  district: string | null
  business: string | null
  tipStatus: "active" | "expired" | "prospect" | null
  fiscalYear: number | null
}

export type IntelligenceCommandId =
  | `surface.${Lowercase<SurfaceId>}`
  | "lens.toggle"
  | "lens.clear"
  | "command.open"
  | "workspace.clear-focus"
  | "workspace.clear-selection"
  | "theme.system"
  | "theme.light"
  | "theme.dark"
