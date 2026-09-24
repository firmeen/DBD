import type { EntityRef, LensState } from "@/lib/intelligence/types"

export const RELATIONSHIP_VIEW_IDS = [
  "explore",
  "neighbourhood",
  "matrix",
  "genealogy",
] as const

export type RelationshipViewId = (typeof RELATIONSHIP_VIEW_IDS)[number]
export type RelationshipAvailability = "ready" | "partial" | "unavailable"
export type RelationshipNodeKind = "company" | "person"
export type RelationshipEdgeKind =
  | "board"
  | "shared-director"
  | "transform"
  | "merger"

export const MAX_PERSON_SEARCH_RESULTS = 50
export const MAX_PERSON_COMPANIES = 500
export const MAX_SHARED_DIRECTORS = 256
export const MAX_RELATIONSHIP_NODES = 3_000
export const MAX_RELATIONSHIP_EDGES = 12_000
export const MAX_MATRIX_COMPANIES = 2_000
export const MAX_MATRIX_PEOPLE = 1_000
export const MAX_MATRIX_MEMBERSHIPS = 50_000
export const MAX_GENEALOGY_NODES = 512
export const MAX_GENEALOGY_EDGES = 1_024

export interface PersonSearchResult {
  id: string
  name: string
  secondary?: string | null
  companyCount: number | null
  activeCompanyCount?: number | null
  tipCompanyCount?: number | null
}

export interface PersonProfile {
  id: string
  name: string
  secondary?: string | null
  companyCount: number | null
  activeCompanyCount?: number | null
  tipCompanyCount?: number | null
}

export interface PersonCompanyRole {
  company: EntityRef
  role?: string | null
  appointedAt?: string | null
  endedAt?: string | null
  companyStatus?: string | null
  tipStatus?: "active" | "expired" | "prospect" | null
  latestFiscalYear?: string | null
  totalIncome?: number | null
  netProfit?: number | null
}

export interface SharedDirectorConnection {
  person: EntityRef
  sharedCompanyCount: number
  companies: EntityRef[]
}

export interface PersonDetailSnapshot {
  availability: RelationshipAvailability
  message?: string
  generatedAt?: string | null
  profile: PersonProfile | null
  companies: PersonCompanyRole[]
  sharedDirectors: SharedDirectorConnection[]
}

export interface CompanyBoardMember {
  person: EntityRef
  role?: string | null
  appointedAt?: string | null
  endedAt?: string | null
  sharedCompanyCount?: number | null
}

export interface CompanyBoardSnapshot {
  availability: RelationshipAvailability
  message?: string
  generatedAt?: string | null
  company: EntityRef | null
  members: CompanyBoardMember[]
}

export interface PersonSearchSnapshot {
  availability: RelationshipAvailability
  message?: string
  generatedAt?: string | null
  results: PersonSearchResult[]
}

export interface RelationshipNode {
  id: string
  kind: RelationshipNodeKind
  label: string
  secondary?: string | null
  degree: number
  tipStatus?: "active" | "expired" | "prospect" | null
  x?: number | null
  y?: number | null
}

export interface RelationshipEdge {
  id: string
  source: string
  target: string
  kind: RelationshipEdgeKind
  label?: string | null
  weight?: number | null
  directed?: boolean
}

export interface RelationshipGraphSnapshot {
  availability: RelationshipAvailability
  message?: string
  generatedAt?: string | null
  layout: "server" | "client"
  nodes: RelationshipNode[]
  edges: RelationshipEdge[]
}

export interface MatrixCompany {
  id: string
  name: string
  secondary?: string | null
  degree: number
  tipStatus?: "active" | "expired" | "prospect" | null
}

export interface MatrixPerson {
  id: string
  name: string
  secondary?: string | null
  degree: number
}

export interface MatrixMembership {
  companyId: string
  personId: string
  role?: string | null
}

export interface BoardMatrixSnapshot {
  availability: RelationshipAvailability
  message?: string
  generatedAt?: string | null
  companies: MatrixCompany[]
  people: MatrixPerson[]
  memberships: MatrixMembership[]
}

export interface GenealogyNode {
  id: string
  company: EntityRef
}

export interface GenealogyEdge {
  id: string
  source: string
  target: string
  kind: "transform" | "merger"
  date?: string | null
  label?: string | null
}

export interface GenealogySnapshot {
  availability: RelationshipAvailability
  message?: string
  generatedAt?: string | null
  nodes: GenealogyNode[]
  edges: GenealogyEdge[]
}

export type RelationshipServingRequest =
  | {
      operation: "person-search"
      query: string
      limit: number
      lens: LensState
    }
  | {
      operation: "person-detail"
      personId: string
      lens: LensState
    }
  | {
      operation: "company-board"
      companyId: string
      lens: LensState
    }
  | {
      operation: "relationship-graph"
      focus: EntityRef | null
      selection: EntityRef[]
      depth: number
      maxNodes: number
      lens: LensState
    }
  | {
      operation: "board-matrix"
      focus: EntityRef | null
      selection: EntityRef[]
      companyQuery: string
      personQuery: string
      maxCompanies: number
      maxPeople: number
      lens: LensState
    }
  | {
      operation: "genealogy"
      companyId: string
      depth: number
      lens: LensState
    }

export interface PeopleSurfaceSnapshot {
  focusedPersonId: string | null
  searchTerm: string
}

export interface RelationshipSurfaceSnapshot {
  view: RelationshipViewId
  depth: number
  matrixCompanyQuery: string
  matrixPersonQuery: string
}

export function personSearchResultToEntity(
  person: PersonSearchResult
): EntityRef {
  return {
    id: person.id,
    kind: "person",
    label: person.name,
    secondary: person.secondary ?? undefined,
  }
}

export function relationshipNodeToEntity(node: RelationshipNode): EntityRef {
  return {
    id: node.id,
    kind: node.kind,
    label: node.label,
    secondary: node.secondary ?? undefined,
  }
}

export function matrixCompanyToEntity(company: MatrixCompany): EntityRef {
  return {
    id: company.id,
    kind: "company",
    label: company.name,
    secondary: company.secondary ?? undefined,
  }
}

export function matrixPersonToEntity(person: MatrixPerson): EntityRef {
  return {
    id: person.id,
    kind: "person",
    label: person.name,
    secondary: person.secondary ?? undefined,
  }
}
