"use client"

import {
  parseAsInteger,
  parseAsString,
  parseAsStringLiteral,
  useQueryStates,
} from "nuqs"

import type { LensState } from "@/lib/intelligence/types"

const tipStatuses = ["active", "expired", "prospect"] as const

const lensParsers = {
  province: parseAsString,
  district: parseAsString,
  business: parseAsString,
  tipStatus: parseAsStringLiteral(tipStatuses),
  fiscalYear: parseAsInteger,
}

export function useGlobalLens() {
  const [lens, setLens] = useQueryStates(lensParsers, {
    history: "push",
    shallow: true,
  })

  const normalized: LensState = {
    province: lens.province,
    district: lens.district,
    business: lens.business,
    tipStatus: lens.tipStatus,
    fiscalYear: lens.fiscalYear,
  }

  const activeEntries = Object.entries(normalized).filter(
    ([, value]) => value !== null && value !== ""
  )

  async function clearLens() {
    await setLens({
      province: null,
      district: null,
      business: null,
      tipStatus: null,
      fiscalYear: null,
    })
  }

  return {
    lens: normalized,
    setLens,
    clearLens,
    activeCount: activeEntries.length,
    activeEntries,
  }
}
