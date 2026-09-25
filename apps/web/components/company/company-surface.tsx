"use client"

import * as React from "react"
import { useQuery, type UseQueryResult } from "@tanstack/react-query"
import {
  AlertTriangle,
  Building2,
  Database,
  LoaderCircle,
  Search,
  X,
} from "lucide-react"
import { AnimatePresence, motion, useReducedMotion } from "motion/react"

import { CompanyDossier } from "./company-dossier"
import { CompanyFinancial } from "./company-financial"
import { CompanyHistory } from "./company-history"
import { CompanyOwnership } from "./company-ownership"
import { CompanyPosition } from "./company-position"
import { useGlobalLens } from "@/hooks/use-global-lens"
import {
  companyEndpointConfigured,
  fetchCompanyDetail,
  searchCompanies,
} from "@/lib/company/adapter"
import { resolveCompanyFinancialYear } from "@/lib/company/financial-year"
import { resolveCompanySurfaceSnapshot } from "@/lib/company/surface-snapshot"
import {
  COMPANY_VIEW_IDS,
  companySearchResultToEntity,
  type CompanyDetailSnapshot,
  type CompanySearchResult,
  type CompanySearchSnapshot,
  type CompanyViewId,
} from "@/lib/company/types"
import type { EntityRef, SurfaceId } from "@/lib/intelligence/types"
import { cn } from "@/lib/utils"
import { useWorkspaceStore } from "@/stores/workspace-store"

const VIEW_META: Record<
  CompanyViewId,
  { short: string; label: string; description: string }
> = {
  dossier: {
    short: "D",
    label: "Dossier",
    description: "Identity and operating context",
  },
  financial: {
    short: "F",
    label: "Financial",
    description: "Trajectory, ratios and statements",
  },
  position: {
    short: "B",
    label: "Position",
    description: "Industry benchmark field",
  },
  ownership: {
    short: "N",
    label: "Ownership",
    description: "Nationality composition",
  },
  history: {
    short: "H",
    label: "History",
    description: "Corporate event rail",
  },
}

export function CompanySurface() {
  const reduceMotion = useReducedMotion()
  const { lens, activeCount } = useGlobalLens()
  const focus = useWorkspaceStore((state) => state.focus)
  const compare = useWorkspaceStore((state) => state.compare)
  const setFocus = useWorkspaceStore((state) => state.setFocus)
  const toggleCompare = useWorkspaceStore((state) => state.toggleCompare)
  const setActiveSurface = useWorkspaceStore((state) => state.setActiveSurface)
  const snapshotPayload = useWorkspaceStore(
    (state) => state.surfaceSnapshots.C?.payload
  )
  const saveSurfaceSnapshot = useWorkspaceStore(
    (state) => state.saveSurfaceSnapshot
  )

  const focusedCompany = focus?.kind === "company" ? focus : null
  const focusedCompanyId = focusedCompany?.id ?? null

  const initialState = React.useMemo(
    () => resolveCompanySurfaceSnapshot(snapshotPayload, focusedCompanyId),
    [focusedCompanyId, snapshotPayload]
  )

  const [view, setView] = React.useState<CompanyViewId>(initialState.view)
  const [searchTerm, setSearchTerm] = React.useState(initialState.searchTerm)
  const [debouncedSearch, setDebouncedSearch] = React.useState(
    initialState.searchTerm
  )
  const [financialYear, setFinancialYear] = React.useState<string | null>(
    initialState.financialYear
  )

  React.useEffect(() => {
    const timeout = window.setTimeout(
      () => setDebouncedSearch(searchTerm.trim()),
      320
    )
    return () => window.clearTimeout(timeout)
  }, [searchTerm])

  React.useEffect(() => {
    saveSurfaceSnapshot("C", {
      focusedCompanyId,
      view,
      searchTerm,
      financialYear,
    })
  }, [
    financialYear,
    focusedCompanyId,
    saveSurfaceSnapshot,
    searchTerm,
    view,
  ])

  const searchQuery = useQuery({
    queryKey: ["tipx", "company", "search", debouncedSearch, lens],
    queryFn: ({ signal }) => searchCompanies(debouncedSearch, lens, signal),
    enabled: debouncedSearch.length >= 2,
    staleTime: 30_000,
    retry: 1,
  })

  const detailQuery = useQuery({
    queryKey: ["tipx", "company", "detail", focusedCompany?.id, lens],
    queryFn: ({ signal }) =>
      fetchCompanyDetail(focusedCompany!.id, lens, signal),
    enabled: focusedCompany !== null,
    staleTime: 60_000,
    retry: 1,
  })

  const detail = detailQuery.data ?? null
  const effectiveYear = resolveCompanyFinancialYear(
    detail?.financial.years ?? [],
    financialYear,
    lens.fiscalYear
  )
  const inCompare =
    focusedCompany !== null &&
    compare.some(
      (entity) =>
        entity.kind === focusedCompany.kind && entity.id === focusedCompany.id
    )

  function focusCompany(entity: EntityRef) {
    setFocus(entity)
    setSearchTerm("")
    setView("dossier")
    setFinancialYear(null)
  }

  function clearCompanyFocus() {
    setFocus(null)
    setView("dossier")
    setFinancialYear(null)
  }

  return (
    <main className="tipx-brand-field relative min-h-svh overflow-x-hidden bg-tipx-canvas text-tipx-ink">
      <div className="tipx-grid fixed inset-0 opacity-25" />
      <div className="tipx-vignette fixed inset-0" />
      <div className="pointer-events-none fixed left-[8%] top-[8%] size-[42vw] max-h-[760px] max-w-[760px] rounded-full bg-tipx-brand/[0.025] blur-3xl" />

      <CompanyCommandBar
        searchTerm={searchTerm}
        debouncedSearch={debouncedSearch}
        onSearchTermChange={setSearchTerm}
        focusedCompany={focusedCompany}
        searchQuery={searchQuery}
        onPickCompany={(company) =>
          focusCompany(companySearchResultToEntity(company))
        }
        onClearFocus={clearCompanyFocus}
      />

      <div className="relative z-10 mx-auto w-full max-w-[1680px] px-4 pb-36 pt-32 sm:px-7 lg:px-10 lg:pt-36">
        {!focusedCompany ? (
          <CompanyDiscovery
            searchTerm={searchTerm}
            endpointConfigured={companyEndpointConfigured()}
            activeLensCount={activeCount}
          />
        ) : detailQuery.isPending ? (
          <CompanyLoading entityLabel={focusedCompany.label} />
        ) : detailQuery.isError ? (
          <CompanyError
            message={
              detailQuery.error instanceof Error
                ? detailQuery.error.message
                : "Company serving request failed."
            }
            onRetry={() => void detailQuery.refetch()}
          />
        ) : detail ? (
          <>
            <div className="mb-5 flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
              <CompanyViewRail
                value={view}
                onChange={setView}
                detail={detail}
              />

              <div className="flex items-center gap-2 self-start lg:self-auto">
                {detail.availability === "partial" ? (
                  <span className="rounded-full border border-tipx-warning/18 bg-tipx-warning/7 px-2.5 py-1.5 text-[9px] uppercase tracking-[0.13em] text-tipx-warning">
                    Partial serving data
                  </span>
                ) : null}
                <button
                  type="button"
                  onClick={() => toggleCompare(focusedCompany)}
                  className={cn(
                    "rounded-xl border px-3 py-2 text-[10px] font-medium transition",
                    inCompare
                      ? "border-tipx-line/22 bg-tipx-brand text-tipx-on-brand"
                      : "border-tipx-line/8 bg-tipx-ink/[0.025] text-tipx-ink/45 hover:bg-tipx-ink/[0.06] hover:text-tipx-ink"
                  )}
                >
                  {inCompare ? "In compare" : "Add to compare"}
                </button>
              </div>
            </div>

            <AnimatePresence mode="wait" initial={false}>
              <motion.div
                key={view}
                initial={reduceMotion ? false : { opacity: 0, y: 7 }}
                animate={{ opacity: 1, y: 0 }}
                exit={reduceMotion ? { opacity: 0 } : { opacity: 0, y: -5 }}
                transition={{ duration: 0.18, ease: [0.2, 0.8, 0.2, 1] }}
              >
                <CompanyView
                  view={view}
                  detail={detail}
                  fallbackEntity={focusedCompany}
                  selectedYear={effectiveYear}
                  onYearChange={setFinancialYear}
                  onFocusCompany={focusCompany}
                  onHandoff={setActiveSurface}
                />
              </motion.div>
            </AnimatePresence>
          </>
        ) : null}
      </div>
    </main>
  )
}

function CompanyCommandBar({
  searchTerm,
  debouncedSearch,
  onSearchTermChange,
  focusedCompany,
  searchQuery,
  onPickCompany,
  onClearFocus,
}: {
  searchTerm: string
  debouncedSearch: string
  onSearchTermChange: (value: string) => void
  focusedCompany: EntityRef | null
  searchQuery: UseQueryResult<CompanySearchSnapshot, Error>
  onPickCompany: (company: CompanySearchResult) => void
  onClearFocus: () => void
}) {
  const results = searchQuery.data?.results ?? []
  const normalizedSearch = searchTerm.trim()
  const showResults = normalizedSearch.length >= 2
  const searchSynchronized =
    showResults && normalizedSearch === debouncedSearch

  return (
    <div className="pointer-events-none fixed left-4 right-[4.75rem] top-4 z-[32] flex items-start gap-2 sm:left-7 sm:right-24 sm:top-7">
      <div className="pointer-events-auto flex min-w-0 flex-1 items-center gap-2 rounded-2xl border border-tipx-line/10 bg-tipx-floating/91 p-1.5 shadow-[0_24px_80px_rgba(0,0,0,.35)] backdrop-blur-2xl sm:max-w-2xl">
        <span className="grid size-10 shrink-0 place-items-center rounded-xl bg-tipx-brand font-mono text-xs font-bold text-tipx-on-brand">
          C
        </span>

        <div className="relative min-w-0 flex-1">
          <Search className="pointer-events-none absolute left-3 top-1/2 size-3.5 -translate-y-1/2 text-tipx-ink/25" />
          <input
            value={searchTerm}
            onChange={(event) => onSearchTermChange(event.target.value)}
            placeholder="Search company or juristic-person number…"
            data-keyboard-input
            className="h-10 w-full rounded-xl border border-transparent bg-tipx-ink/[0.025] pl-9 pr-9 text-xs text-tipx-ink outline-none transition placeholder:text-tipx-ink/21 focus:border-tipx-line/10 focus:bg-tipx-ink/[0.045]"
          />
          {searchTerm ? (
            <button
              type="button"
              onClick={() => onSearchTermChange("")}
              className="absolute right-2 top-1/2 grid size-7 -translate-y-1/2 place-items-center rounded-lg text-tipx-ink/24 hover:bg-tipx-ink/6 hover:text-tipx-ink"
              aria-label="Clear company search"
            >
              <X className="size-3.5" />
            </button>
          ) : null}

          {showResults ? (
            <CompanySearchPopover
              queryState={searchQuery}
              results={searchSynchronized ? results : []}
              synchronized={searchSynchronized}
              onPickCompany={onPickCompany}
            />
          ) : null}
        </div>
      </div>

      {focusedCompany ? (
        <div className="pointer-events-auto flex min-w-0 items-center rounded-2xl border border-tipx-line/9 bg-tipx-floating/86 p-1.5 backdrop-blur-xl lg:max-w-sm lg:gap-2">
          <div className="hidden min-w-0 px-3 lg:block">
            <p className="truncate text-[10px] font-medium text-tipx-ink/64">
              {focusedCompany.label}
            </p>
            <p className="mt-0.5 truncate font-mono text-[8px] text-tipx-ink/24">
              {focusedCompany.id}
            </p>
          </div>
          <button
            type="button"
            onClick={onClearFocus}
            className="grid size-9 shrink-0 place-items-center rounded-xl text-tipx-ink/36 transition hover:bg-tipx-ink/6 hover:text-tipx-ink"
            aria-label={`Clear focused company: ${focusedCompany.label}`}
            title="Return to company discovery"
          >
            <X className="size-3.5" />
          </button>
        </div>
      ) : null}
    </div>
  )
}

function CompanySearchPopover({
  queryState,
  results,
  synchronized,
  onPickCompany,
}: {
  queryState: UseQueryResult<CompanySearchSnapshot, Error>
  results: CompanySearchResult[]
  synchronized: boolean
  onPickCompany: (company: CompanySearchResult) => void
}) {
  const data = queryState.data

  return (
    <div className="absolute left-0 right-0 top-[calc(100%+10px)] overflow-hidden rounded-2xl border border-tipx-line/10 bg-tipx-floating/98 shadow-[0_28px_90px_rgba(0,0,0,.55)] backdrop-blur-2xl">
      {!synchronized ? (
        <SearchState
          icon={LoaderCircle}
          title="Preparing updated query"
          description="Waiting for the visible search text to reach the debounced serving query. Previous results are intentionally hidden."
        />
      ) : queryState.isFetching ? (
        <div className="flex items-center gap-2 border-b border-tipx-line/7 px-4 py-2 text-[9px] uppercase tracking-[0.14em] text-tipx-ink/25">
          <LoaderCircle className="size-3 animate-spin" />
          Searching serving model
        </div>
      ) : null}

      {!synchronized || queryState.isPending ? null : queryState.isError ? (
        <SearchState
          icon={AlertTriangle}
          title="Search request failed"
          description={
            queryState.error instanceof Error
              ? queryState.error.message
              : "Company serving search failed."
          }
        />
      ) : data?.availability === "unavailable" ? (
        <SearchState
          icon={Database}
          title="Company serving unavailable"
          description={data.message || "No production company search endpoint."}
        />
      ) : results.length > 0 ? (
        <div className="max-h-[420px] overflow-auto p-1.5">
          {results.map((company) => (
            <button
              key={company.id}
              type="button"
              onClick={() => onPickCompany(company)}
              className="flex w-full items-center gap-3 rounded-xl px-3 py-3 text-left transition hover:bg-tipx-ink/[0.055]"
            >
              <span className="grid size-9 shrink-0 place-items-center rounded-xl border border-tipx-line/8 bg-tipx-ink/[0.035] text-xs font-semibold text-tipx-ink/55">
                {company.nameTh.trim().charAt(0).toUpperCase() || "?"}
              </span>
              <span className="min-w-0 flex-1">
                <span className="block truncate text-[11px] font-medium text-tipx-ink/68">
                  {company.nameTh}
                </span>
                <span className="mt-0.5 block truncate text-[9px] text-tipx-ink/27">
                  {[company.id, company.businessType, company.province]
                    .filter(Boolean)
                    .join(" · ")}
                </span>
              </span>
              {company.latestFiscalYear ? (
                <span className="shrink-0 font-mono text-[8px] text-tipx-ink/20">
                  FY {company.latestFiscalYear}
                </span>
              ) : null}
            </button>
          ))}
        </div>
      ) : queryState.isSuccess ? (
        <SearchState
          icon={Search}
          title="No matching company"
          description="The production serving response returned no company for this query and Lens."
        />
      ) : (
        <SearchState
          icon={Search}
          title="Type at least two characters"
          description="Search resolves canonical company identity before opening the workbench."
        />
      )}
    </div>
  )
}

function CompanyDiscovery({
  searchTerm,
  endpointConfigured,
  activeLensCount,
}: {
  searchTerm: string
  endpointConfigured: boolean
  activeLensCount: number
}) {
  return (
    <section className="grid min-h-[calc(100svh-12rem)] place-items-center">
      <div className="max-w-2xl text-center">
        <span className="mx-auto grid size-12 place-items-center rounded-2xl border border-tipx-line/9 bg-tipx-ink/[0.025]">
          <Building2 className="size-5 text-tipx-ink/38" />
        </span>
        <p className="mt-6 font-mono text-[10px] uppercase tracking-[0.28em] text-tipx-ink/24">
          C / Company Intelligence
        </p>
        <h1 className="mt-3 text-balance text-4xl font-semibold tracking-[-0.045em] text-tipx-ink sm:text-6xl">
          Start with an entity,
          <br />
          not a dashboard.
        </h1>
        <p className="mx-auto mt-5 max-w-xl text-pretty text-[12px] leading-6 text-tipx-ink/34">
          Search a canonical company identity or arrive from Map. Financial,
          benchmark, ownership and corporate-history representations remain
          attached to the same entity context.
        </p>

        <div className="mx-auto mt-7 flex w-fit items-center gap-4 rounded-2xl border border-tipx-line/8 bg-tipx-ink/[0.02] px-4 py-3 text-[9px] uppercase tracking-[0.13em] text-tipx-ink/27">
          <span className="inline-flex items-center gap-2">
            <span
              className={cn(
                "size-1.5 rounded-full",
                endpointConfigured ? "bg-tipx-success/70" : "bg-tipx-ink/16"
              )}
            />
            {endpointConfigured ? "Serving configured" : "Serving unavailable"}
          </span>
          <span className="h-3 w-px bg-tipx-ink/8" />
          <span>Lens {activeLensCount}</span>
          {searchTerm ? (
            <>
              <span className="h-3 w-px bg-tipx-ink/8" />
              <span className="max-w-36 truncate normal-case tracking-normal">
                “{searchTerm}”
              </span>
            </>
          ) : null}
        </div>
      </div>
    </section>
  )
}

function CompanyViewRail({
  value,
  onChange,
  detail,
}: {
  value: CompanyViewId
  onChange: (view: CompanyViewId) => void
  detail: CompanyDetailSnapshot
}) {
  const capability: Record<CompanyViewId, boolean> = {
    dossier: true,
    financial: detail.capabilities.financials,
    position: detail.capabilities.industryBenchmark,
    ownership: detail.capabilities.nationality,
    history: detail.capabilities.corporateHistory,
  }

  return (
    <nav
      aria-label="Company intelligence representations"
      className="flex max-w-full items-center gap-1 overflow-x-auto rounded-2xl border border-tipx-line/8 bg-tipx-surface/72 p-1 backdrop-blur-xl"
    >
      {COMPANY_VIEW_IDS.map((id) => {
        const meta = VIEW_META[id]
        const active = value === id

        return (
          <button
            key={id}
            type="button"
            onClick={() => onChange(id)}
            className={cn(
              "group flex shrink-0 items-center gap-2 rounded-xl px-3 py-2 transition",
              active
                ? "bg-tipx-brand text-tipx-on-brand"
                : "text-tipx-ink/35 hover:bg-tipx-ink/[0.055] hover:text-tipx-ink/70"
            )}
          >
            <span
              className={cn(
                "grid size-5 place-items-center rounded-md border font-mono text-[8px] font-bold",
                active ? "border-tipx-brand/18" : "border-tipx-line/8"
              )}
            >
              {meta.short}
            </span>
            <span className="text-left">
              <span className="block text-[10px] font-medium">
                {meta.label}
              </span>
              <span
                className={cn(
                  "hidden text-[8px] xl:block",
                  active ? "text-tipx-on-brand/42" : "text-tipx-ink/19"
                )}
              >
                {capability[id] ? meta.description : "Capability unavailable"}
              </span>
            </span>
          </button>
        )
      })}
    </nav>
  )
}

function CompanyView({
  view,
  detail,
  fallbackEntity,
  selectedYear,
  onYearChange,
  onFocusCompany,
  onHandoff,
}: {
  view: CompanyViewId
  detail: CompanyDetailSnapshot
  fallbackEntity: EntityRef
  selectedYear: string | null
  onYearChange: (year: string) => void
  onFocusCompany: (entity: EntityRef) => void
  onHandoff: (surface: SurfaceId) => void
}) {
  switch (view) {
    case "financial":
      return (
        <CompanyFinancial
          years={detail.financial.years}
          series={detail.financial.series}
          selectedYear={selectedYear}
          onYearChange={onYearChange}
        />
      )
    case "position":
      return <CompanyPosition industry={detail.industry} />
    case "ownership":
      return (
        <CompanyOwnership
          current={detail.nationality.current}
          history={detail.nationality.history}
        />
      )
    case "history":
      return (
        <CompanyHistory
          events={detail.history}
          onFocusCompany={onFocusCompany}
          onHandoff={onHandoff}
        />
      )
    case "dossier":
    default:
      return (
        <CompanyDossier
          detail={detail}
          fallbackEntity={fallbackEntity}
          onHandoff={onHandoff}
        />
      )
  }
}

function CompanyLoading({ entityLabel }: { entityLabel: string }) {
  return (
    <div className="grid min-h-[calc(100svh-13rem)] place-items-center text-center">
      <div>
        <LoaderCircle className="mx-auto size-6 animate-spin text-tipx-ink/28" />
        <p className="mt-4 text-sm font-medium text-tipx-ink/52">
          Resolving {entityLabel}
        </p>
        <p className="mt-1 text-[10px] text-tipx-ink/25">
          Loading company intelligence contract
        </p>
      </div>
    </div>
  )
}

function CompanyError({
  message,
  onRetry,
}: {
  message: string
  onRetry: () => void
}) {
  return (
    <div className="grid min-h-[calc(100svh-13rem)] place-items-center text-center">
      <div>
        <AlertTriangle className="mx-auto size-6 text-tipx-warning/80" />
        <p className="mt-4 text-sm font-medium text-tipx-ink/52">
          Company serving request failed
        </p>
        <p className="mx-auto mt-2 max-w-lg text-[11px] leading-5 text-tipx-ink/28">
          {message}
        </p>
        <button
          type="button"
          onClick={onRetry}
          className="mt-5 rounded-xl border border-tipx-line/9 bg-tipx-ink/[0.035] px-4 py-2 text-[10px] text-tipx-ink/50 hover:bg-tipx-ink/[0.07] hover:text-tipx-ink"
        >
          Retry
        </button>
      </div>
    </div>
  )
}

function SearchState({
  icon: Icon,
  title,
  description,
}: {
  icon: typeof Search
  title: string
  description: string
}) {
  return (
    <div className="flex items-start gap-3 p-4">
      <span className="grid size-8 shrink-0 place-items-center rounded-xl border border-tipx-line/8 bg-tipx-ink/[0.025]">
        <Icon className="size-3.5 text-tipx-ink/30" />
      </span>
      <span>
        <span className="block text-[11px] font-medium text-tipx-ink/55">
          {title}
        </span>
        <span className="mt-1 block text-[9px] leading-4 text-tipx-ink/27">
          {description}
        </span>
      </span>
    </div>
  )
}
