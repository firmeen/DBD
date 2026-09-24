import type { EntityRef } from "@/lib/intelligence/types"

import { MAX_SPATIAL_EXACT_COMPANY_IDS } from "./types"

export function exactCompanyIdsForSpatial(selection: EntityRef[]) {
  const ids = [
    ...new Set(
      selection
        .filter((entity) => entity.kind === "company")
        .map((entity) => entity.id)
    ),
  ]

  return ids.length <= MAX_SPATIAL_EXACT_COMPANY_IDS ? ids : []
}
