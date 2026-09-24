import {
  parseBoardMatrixSnapshot,
  parseCompanyBoardSnapshot,
  parseGenealogySnapshot,
  parsePersonDetailSnapshot,
  parsePersonSearchSnapshot,
  parseRelationshipGraphSnapshot,
} from "./contract"
import {
  MAX_MATRIX_COMPANIES,
  MAX_MATRIX_PEOPLE,
  MAX_PERSON_SEARCH_RESULTS,
  MAX_RELATIONSHIP_NODES,
  type BoardMatrixSnapshot,
  type CompanyBoardSnapshot,
  type GenealogySnapshot,
  type PersonDetailSnapshot,
  type PersonSearchSnapshot,
  type RelationshipGraphSnapshot,
  type RelationshipServingRequest,
} from "./types"
import type { EntityRef, LensState } from "@/lib/intelligence/types"

export function getRelationshipEndpoint() {
  const value = process.env.NEXT_PUBLIC_TIPX_RELATIONSHIP_ENDPOINT?.trim()
  return value || null
}

export function relationshipEndpointConfigured() {
  return getRelationshipEndpoint() !== null
}

async function postRelationshipRequest<T>(
  request: RelationshipServingRequest,
  parser: (input: unknown) => T,
  signal?: AbortSignal
): Promise<T> {
  const endpoint = getRelationshipEndpoint()
  if (!endpoint) {
    throw new Error("TIPX relationship serving endpoint is not configured")
  }

  const response = await fetch(endpoint, {
    method: "POST",
    headers: {
      accept: "application/json",
      "content-type": "application/json",
    },
    body: JSON.stringify(request),
    cache: "no-store",
    signal,
  })

  if (!response.ok) {
    throw new Error(
      `Relationship serving request failed with HTTP ${response.status}`
    )
  }

  return parser(await response.json())
}

export async function searchPeople(
  query: string,
  lens: LensState,
  signal?: AbortSignal
): Promise<PersonSearchSnapshot> {
  if (!getRelationshipEndpoint()) {
    return unavailablePersonSearch(
      "Center Database relationship serving is not configured. Person search never fabricates board-member identities."
    )
  }

  return postRelationshipRequest(
    {
      operation: "person-search",
      query,
      limit: MAX_PERSON_SEARCH_RESULTS,
      lens,
    },
    parsePersonSearchSnapshot,
    signal
  )
}

export async function fetchPersonDetail(
  personId: string,
  lens: LensState,
  signal?: AbortSignal
): Promise<PersonDetailSnapshot> {
  if (!getRelationshipEndpoint()) {
    return {
      availability: "unavailable",
      message:
        "Person detail serving is unavailable. The canonical person focus remains valid without invented company relationships.",
      generatedAt: null,
      profile: null,
      companies: [],
      sharedDirectors: [],
    }
  }

  return postRelationshipRequest(
    {
      operation: "person-detail",
      personId,
      lens,
    },
    (input) => parsePersonDetailSnapshot(input, personId),
    signal
  )
}

export async function fetchCompanyBoard(
  companyId: string,
  lens: LensState,
  signal?: AbortSignal
): Promise<CompanyBoardSnapshot> {
  if (!getRelationshipEndpoint()) {
    return {
      availability: "unavailable",
      message:
        "Company-board serving is unavailable. No director identities are inferred in the browser.",
      generatedAt: null,
      company: null,
      members: [],
    }
  }

  return postRelationshipRequest(
    {
      operation: "company-board",
      companyId,
      lens,
    },
    (input) => parseCompanyBoardSnapshot(input, companyId),
    signal
  )
}

export async function fetchRelationshipGraph(
  focus: EntityRef | null,
  selection: EntityRef[],
  depth: number,
  lens: LensState,
  signal?: AbortSignal
): Promise<RelationshipGraphSnapshot> {
  if (!getRelationshipEndpoint()) {
    return unavailableGraph(
      "Relationship graph serving is unavailable. The Web UI does not construct production networks from raw source rows."
    )
  }

  return postRelationshipRequest(
    {
      operation: "relationship-graph",
      focus,
      selection,
      depth,
      maxNodes: MAX_RELATIONSHIP_NODES,
      lens,
    },
    parseRelationshipGraphSnapshot,
    signal
  )
}

export async function fetchBoardMatrix(
  focus: EntityRef | null,
  selection: EntityRef[],
  companyQuery: string,
  personQuery: string,
  lens: LensState,
  signal?: AbortSignal
): Promise<BoardMatrixSnapshot> {
  if (!getRelationshipEndpoint()) {
    return {
      availability: "unavailable",
      message:
        "Board Matrix serving is unavailable. Matrix membership cells are never invented client-side.",
      generatedAt: null,
      companies: [],
      people: [],
      memberships: [],
    }
  }

  return postRelationshipRequest(
    {
      operation: "board-matrix",
      focus,
      selection,
      companyQuery,
      personQuery,
      maxCompanies: MAX_MATRIX_COMPANIES,
      maxPeople: MAX_MATRIX_PEOPLE,
      lens,
    },
    parseBoardMatrixSnapshot,
    signal
  )
}

export async function fetchGenealogy(
  companyId: string,
  depth: number,
  lens: LensState,
  signal?: AbortSignal
): Promise<GenealogySnapshot> {
  if (!getRelationshipEndpoint()) {
    return {
      availability: "unavailable",
      message:
        "Transformation genealogy serving is unavailable. Directed history is not inferred from display labels.",
      generatedAt: null,
      nodes: [],
      edges: [],
    }
  }

  return postRelationshipRequest(
    {
      operation: "genealogy",
      companyId,
      depth,
      lens,
    },
    parseGenealogySnapshot,
    signal
  )
}

function unavailablePersonSearch(message: string): PersonSearchSnapshot {
  return {
    availability: "unavailable",
    message,
    generatedAt: null,
    results: [],
  }
}

function unavailableGraph(message: string): RelationshipGraphSnapshot {
  return {
    availability: "unavailable",
    message,
    generatedAt: null,
    layout: "client",
    nodes: [],
    edges: [],
  }
}
