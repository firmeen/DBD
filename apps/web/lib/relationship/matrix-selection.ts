import type { EntityRef } from "@/lib/intelligence/types"

import {
  matrixCompanyToEntity,
  matrixPersonToEntity,
  type MatrixCompany,
  type MatrixPerson,
} from "./types"

export function matrixRegionSelection(
  companies: MatrixCompany[],
  people: MatrixPerson[],
  limit: number
): EntityRef[] {
  if (limit <= 0 || companies.length === 0 || people.length === 0) return []

  const safeLimit = Math.max(2, Math.floor(limit))
  let companyTake = Math.min(
    companies.length,
    Math.max(1, Math.floor(safeLimit / 2))
  )
  let personTake = Math.min(
    people.length,
    Math.max(1, safeLimit - companyTake)
  )

  let remaining = safeLimit - companyTake - personTake

  if (remaining > 0) {
    const extraCompanies = Math.min(
      remaining,
      companies.length - companyTake
    )
    companyTake += extraCompanies
    remaining -= extraCompanies
  }

  if (remaining > 0) {
    personTake += Math.min(remaining, people.length - personTake)
  }

  return [
    ...companies.slice(0, companyTake).map(matrixCompanyToEntity),
    ...people.slice(0, personTake).map(matrixPersonToEntity),
  ]
}
