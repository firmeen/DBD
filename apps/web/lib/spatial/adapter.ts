import { parseSpatialSnapshot } from "./contract"
import type { SpatialQuery, SpatialSnapshot } from "./types"

const NO_CAPABILITIES = {
  administrativeGeometry: false,
  preciseCompanyGeometry: false,
  branches: false,
  h3Aggregates: false,
  opportunityOverlay: false,
} as const

export function getSpatialEndpoint() {
  const value = process.env.NEXT_PUBLIC_TIPX_SPATIAL_ENDPOINT?.trim()
  return value || null
}

export function spatialEndpointConfigured() {
  return getSpatialEndpoint() !== null
}

export async function fetchSpatialSnapshot(
  query: SpatialQuery,
  signal?: AbortSignal
): Promise<SpatialSnapshot> {
  const endpoint = getSpatialEndpoint()

  if (!endpoint) {
    return {
      availability: "unavailable",
      message:
        "Center Database spatial serving contract is not configured. The map remains operational without inventing company geometry.",
      generatedAt: null,
      capabilities: NO_CAPABILITIES,
      totals: {
        companies: null,
        active: null,
        expired: null,
        prospect: null,
      },
      companies: [],
      branches: [],
      h3Cells: [],
      areas: [],
    }
  }

  const response = await fetch(endpoint, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      accept: "application/json",
    },
    body: JSON.stringify(query),
    cache: "no-store",
    signal,
  })

  if (!response.ok) {
    throw new Error(
      `Spatial serving request failed with HTTP ${response.status}`
    )
  }

  return parseSpatialSnapshot(await response.json())
}
