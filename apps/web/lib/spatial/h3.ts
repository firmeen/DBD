import { latLngToCell } from "h3-js"

import type { SpatialCompanyPoint, SpatialH3Cell } from "./types"

const MAX_CLIENT_H3_POINTS = 12_000

export function aggregateCompanyPointsToH3(
  companies: SpatialCompanyPoint[],
  resolution: number
): SpatialH3Cell[] {
  if (
    companies.length === 0 ||
    companies.length > MAX_CLIENT_H3_POINTS
  ) {
    return []
  }

  const cells = new Map<string, SpatialH3Cell>()

  for (const company of companies) {
    const [longitude, latitude] = company.coordinates
    const index = latLngToCell(latitude, longitude, resolution)
    const current = cells.get(index) ?? {
      index,
      count: 0,
      active: 0,
      expired: 0,
      prospect: 0,
    }

    current.count += 1

    if (company.tipStatus === "active") current.active += 1
    if (company.tipStatus === "expired") current.expired += 1
    if (company.tipStatus === "prospect") current.prospect += 1

    cells.set(index, current)
  }

  return [...cells.values()]
}

export function canAggregateH3InBrowser(companyCount: number) {
  return companyCount > 0 && companyCount <= MAX_CLIENT_H3_POINTS
}
