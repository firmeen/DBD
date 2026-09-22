import type { SurfaceId } from "./types"

export interface IntelligenceSurface {
  id: SurfaceId
  name: string
  shortName: string
  description: string
  ownerIssue: number
  status: "foundation" | "planned"
}

export const SURFACES: readonly IntelligenceSurface[] = [
  {
    id: "M",
    name: "Main Spatial Intelligence",
    shortName: "Spatial",
    description: "Geographic operating surface and the default analytical home.",
    ownerIssue: 12,
    status: "planned",
  },
  {
    id: "C",
    name: "Company Intelligence",
    shortName: "Company",
    description: "Company, financial, industry and corporate-history investigation.",
    ownerIssue: 11,
    status: "planned",
  },
  {
    id: "P",
    name: "People Intelligence",
    shortName: "People",
    description: "Person and board-member investigation with shared entity context.",
    ownerIssue: 29,
    status: "planned",
  },
  {
    id: "R",
    name: "Relationship Intelligence",
    shortName: "Relation",
    description: "Graph, matrix and directed relationship representations.",
    ownerIssue: 29,
    status: "planned",
  },
  {
    id: "A",
    name: "Population Analytics",
    shortName: "Analytics",
    description: "Coordinated population-level analytical views.",
    ownerIssue: 30,
    status: "planned",
  },
  {
    id: "O",
    name: "Opportunity Intelligence",
    shortName: "Opportunity",
    description: "TIP, Sale-Detail, prospect and territory opportunity analysis.",
    ownerIssue: 30,
    status: "planned",
  },
] as const

export const SURFACE_BY_ID = Object.fromEntries(
  SURFACES.map((surface) => [surface.id, surface])
) as Record<SurfaceId, IntelligenceSurface>

export function isSurfaceId(value: string): value is SurfaceId {
  return Object.prototype.hasOwnProperty.call(SURFACE_BY_ID, value.toUpperCase())
}
