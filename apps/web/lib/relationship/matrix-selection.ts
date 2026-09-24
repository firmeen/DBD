import type { EntityRef } from "@/lib/intelligence/types"

export function capMatrixRegionSelection(
  companies: EntityRef[],
  people: EntityRef[],
  maxEntities: number
) {
  if (maxEntities < 2) {
    throw new Error("Matrix region selection budget must preserve both axes")
  }

  if (companies.length === 0 || people.length === 0) {
    return [...companies, ...people].slice(0, maxEntities)
  }

  if (companies.length + people.length <= maxEntities) {
    return [...companies, ...people]
  }

  const basePerAxis = Math.max(1, Math.floor(maxEntities / 2))
  let companyBudget = Math.min(companies.length, basePerAxis)
  let peopleBudget = Math.min(people.length, basePerAxis)

  let remaining = maxEntities - companyBudget - peopleBudget

  if (remaining > 0 && companies.length > companyBudget) {
    const extra = Math.min(remaining, companies.length - companyBudget)
    companyBudget += extra
    remaining -= extra
  }

  if (remaining > 0 && people.length > peopleBudget) {
    peopleBudget += Math.min(remaining, people.length - peopleBudget)
  }

  return [
    ...companies.slice(0, companyBudget),
    ...people.slice(0, peopleBudget),
  ]
}
