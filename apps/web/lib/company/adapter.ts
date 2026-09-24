import {
  parseCompanyDetailSnapshot,
  parseCompanySearchSnapshot,
} from "./contract"
import type {
  CompanyDetailSnapshot,
  CompanySearchSnapshot,
  CompanyServingRequest,
} from "./types"
import type { LensState } from "@/lib/intelligence/types"

const NO_CAPABILITIES = {
  financials: false,
  industryBenchmark: false,
  nationality: false,
  corporateHistory: false,
  tipContext: false,
  boardSummary: false,
} as const

export function getCompanyEndpoint() {
  const value = process.env.NEXT_PUBLIC_TIPX_COMPANY_ENDPOINT?.trim()
  return value || null
}

export function companyEndpointConfigured() {
  return getCompanyEndpoint() !== null
}

async function postCompanyRequest<T>(
  request: CompanyServingRequest,
  parser: (input: unknown) => T,
  signal?: AbortSignal
): Promise<T> {
  const endpoint = getCompanyEndpoint()
  if (!endpoint) {
    throw new Error("TIPX company serving endpoint is not configured")
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
      `Company serving request failed with HTTP ${response.status}`
    )
  }

  return parser(await response.json())
}

export async function searchCompanies(
  query: string,
  lens: LensState,
  signal?: AbortSignal
): Promise<CompanySearchSnapshot> {
  if (!getCompanyEndpoint()) {
    return {
      availability: "unavailable",
      message:
        "Center Database company serving contract is not configured. Search never fabricates company results.",
      generatedAt: null,
      results: [],
    }
  }

  return postCompanyRequest(
    {
      operation: "search",
      query,
      limit: 30,
      lens,
    },
    parseCompanySearchSnapshot,
    signal
  )
}

export async function fetchCompanyDetail(
  companyId: string,
  lens: LensState,
  signal?: AbortSignal
): Promise<CompanyDetailSnapshot> {
  if (!getCompanyEndpoint()) {
    return {
      availability: "unavailable",
      message:
        "Center Database company serving contract is not configured. The focused entity remains available without inventing financial values.",
      generatedAt: null,
      capabilities: NO_CAPABILITIES,
      profile: null,
      financial: {
        years: [],
        series: [],
      },
      industry: null,
      nationality: {
        current: [],
        history: [],
      },
      history: [],
      tipContext: null,
      boardSummary: null,
    }
  }

  return postCompanyRequest(
    {
      operation: "detail",
      companyId,
      lens,
    },
    parseCompanyDetailSnapshot,
    signal
  )
}
