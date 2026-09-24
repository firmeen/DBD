"use client"

import * as React from "react"
import { useQuery } from "@tanstack/react-query"
import {
  Activity,
  AlertTriangle,
  Database,
  FilterX,
  LoaderCircle,
  Map as MapIcon,
  Network,
} from "lucide-react"

import { AnalyticsCandidates } from "./analytics-candidates"
import { AnalyticsCompare } from "./analytics-compare"
import { AnalyticsDistributions } from "./analytics-distributions"
import { AnalyticsTrajectory } from "./analytics-trajectory"
import { OpportunityField } from "./opportunity-field"
import { useGlobalLens } from "@/hooks/use-global-lens"
import {
  analyticsEndpointConfigured,
  fetchAnalyticsSnapshot,
} from "@/lib/analytics/adapter"
import { formatAnalyticsMetric, formatOpportunityScore } from "@/lib/analytics/format"
import { parseAnalyticsSurfaceSnapshot } from "@/lib/analytics/surface-snapshot"
import type {
  AnalyticsCompanyRow,
  AnalyticsDerivedSubset,
  AnalyticsDistribution,
  AnalyticsFilter,
  AnalyticsMode,
  AnalyticsSnapshot,
  AnalyticsSummary,
} from "@/lib/analytics/types"
import { MAX_ANALYTICS_GLOBAL_SELECTION } from "@/lib/analytics/types"
import { computeAnalyticsSubsetOffMainThread } from "@/lib/analytics/worker-client"
import type { EntityRef, PopulationRef, SurfaceId } from "@/lib/intelligence/types"
import { cn } from "@/lib/utils"
import { useWorkspaceStore } from "@/stores/workspace-store"

const EMPTY_SUMMARY: AnalyticsSummary = {
  companyCount: null,
  active: null,
  expired: null,
  prospect: null,
  tipPenetration: null,
  totalIncome: null,
  medianIncome: null,
  totalPremium: null,
  totalLoss: null,
  lossRatio: null,
  averageOpportunityScore: null,
}

const EMPTY_SNAPSHOT: AnalyticsSnapshot = {
  availability: "unavailable",
  message: "Analytics population has not been requested.",
  generatedAt: null,
  population: null,
  capabilities: {
    populationAnalytics: false,
    opportunity: false,
    productPerformance: false,
    territory: false,
    localCrossFilter: false,
  },
  summary: EMPTY_SUMMARY,
  distributions: [],
  companies: [],
  products: [],
  territories: [],
  series: [],
}

export function AnalyticsSurface({ mode }: { mode: AnalyticsMode }) {
  const surfaceId: "A" | "O" = mode === "population" ? "A" : "O"
  const { lens } = useGlobalLens()
  const focus = useWorkspaceStore((state) => state.focus)
  const selection = useWorkspaceStore((state) => state.selection)
  const compare = useWorkspaceStore((state) => state.compare)
  const population = useWorkspaceStore((state) => state.population)
  const setFocus = useWorkspaceStore((state) => state.setFocus)
  const setSelection = useWorkspaceStore((state) => state.setSelection)
  const toggleSelection = useWorkspaceStore((state) => state.toggleSelection)
  const toggleCompare = useWorkspaceStore((state) => state.toggleCompare)
  const setPopulation = useWorkspaceStore((state) => state.setPopulation)
  const setActiveSurface = useWorkspaceStore((state) => state.setActiveSurface)
  const saveSurfaceSnapshot = useWorkspaceStore(
    (state) => state.saveSurfaceSnapshot
  )
  const snapshotPayload = useWorkspaceStore(
    (state) => state.surfaceSnapshots[surfaceId]?.payload
  )

  const initialState = React.useMemo(
    () => parseAnalyticsSurfaceSnapshot(snapshotPayload, mode),
    [mode, snapshotPayload]
  )
  const [filters, setFilters] = React.useState(initialState.filters)
  const [candidateRailOpen, setCandidateRailOpen] = React.useState(
    initialState.candidateRailOpen
  )

  React.useEffect(() => {
    saveSurfaceSnapshot(surfaceId, {
      mode,
      filters,
      candidateRailOpen,
    })
  }, [candidateRailOpen, filters, mode, saveSurfaceSnapshot, surfaceId])

  const query = useQuery({
    queryKey: [
      "tipx",
      "analytics",
      mode,
      lens,
      focus,
      selection,
      compare,
      population,
    ],
    queryFn: ({ signal }) =>
      fetchAnalyticsSnapshot(
        mode,
        { lens, focus, selection, compare, population },
        signal
      ),
    staleTime: 45_000,
    retry: 1,
  })

  const snapshot = query.data ?? EMPTY_SNAPSHOT

  const derivedQuery = useQuery({
    queryKey: [
      "tipx",
      "analytics",
      "derived",
      mode,
      query.dataUpdatedAt,
      filters,
    ],
    queryFn: () =>
      computeAnalyticsSubsetOffMainThread(snapshot.companies, filters),
    enabled:
      snapshot.capabilities.localCrossFilter &&
      snapshot.companies.length > 0 &&
      filters.length > 0,
    staleTime: Number.POSITIVE_INFINITY,
  })

  const derived = derivedQuery.data ?? null
  const filteredCompanies = React.useMemo(
    () => resolveFilteredCompanies(snapshot.companies, filters, derived),
    [derived, filters, snapshot.companies]
  )
  const effectiveSummary =
    filters.length > 0 ? derived?.summary ?? EMPTY_SUMMARY : snapshot.summary
  const effectiveDistributions = React.useMemo(
    () =>
      filters.length > 0 && derived
        ? derivedDistributions(derived)
        : snapshot.distributions,
    [derived, filters.length, snapshot.distributions]
  )

  const selectedCompanyIds = React.useMemo(
    () =>
      new Set(
        selection
          .filter((entity) => entity.kind === "company")
          .map((entity) => entity.id)
      ),
    [selection]
  )
  const comparedCompanyIds = React.useMemo(
    () =>
      new Set(
        compare
          .filter((entity) => entity.kind === "company")
          .map((entity) => entity.id)
      ),
    [compare]
  )

  function toggleFilter(filter: AnalyticsFilter) {
    if (!snapshot.capabilities.localCrossFilter) return

    setFilters((current) => {
      const existing = current.find(
        (candidate) => candidate.dimension === filter.dimension
      )
      if (existing?.value === filter.value) {
        return current.filter(
          (candidate) => candidate.dimension !== filter.dimension
        )
      }

      return [
        ...current.filter(
          (candidate) => candidate.dimension !== filter.dimension
        ),
        filter,
      ]
    })
  }

  function activatePopulation(nextPopulation: PopulationRef) {
    setPopulation(nextPopulation)
    setFilters([])
  }

  function focusCompany(company: AnalyticsCompanyRow) {
    setFocus(companyEntity(company))
  }

  function openCompany(company: AnalyticsCompanyRow) {
    setFocus(companyEntity(company))
    setActiveSurface("C")
  }

  function handoff(surface: Extract<SurfaceId, "M" | "R">) {
    if (filters.length === 0 && snapshot.population) {
      setPopulation(snapshot.population)
      setActiveSurface(surface)
      return
    }

    if (
      filteredCompanies.length > 0 &&
      filteredCompanies.length <= MAX_ANALYTICS_GLOBAL_SELECTION
    ) {
      setPopulation(null)
      setSelection(filteredCompanies.map(companyEntity))
      setActiveSurface(surface)
    }
  }

  const canUseServedPopulation =
    filters.length === 0 && snapshot.population !== null
  const exactHandoffCount = filteredCompanies.length
  const handoffBlocked =
    !canUseServedPopulation &&
    (exactHandoffCount === 0 ||
      exactHandoffCount > MAX_ANALYTICS_GLOBAL_SELECTION ||
      derivedQuery.isFetching)

  return (
    <main className="relative min-h-svh overflow-x-hidden bg-[#070a0d] text-white">
      <div className="tipx-grid pointer-events-none fixed inset-0 opacity-20" />
      <div className="tipx-vignette pointer-events-none fixed inset-0" />

      <AnalyticsControlDeck
        mode={mode}
        endpointConfigured={analyticsEndpointConfigured()}
        population={population}
        servedPopulation={snapshot.population}
        filters={filters}
        loadedCount={filteredCompanies.length}
        computing={derivedQuery.isFetching}
        handoffBlocked={handoffBlocked}
        onModeChange={(nextMode) =>
          setActiveSurface(nextMode === "population" ? "A" : "O")
        }
        onClearFilters={() => setFilters([])}
        onPopulation={activatePopulation}
        onHandoff={handoff}
      />

      <div className="relative z-10 mx-auto max-w-[1800px] px-4 pb-24 pt-32 sm:px-7 sm:pt-36">
        <header className="mb-5 flex flex-col gap-3 border-b border-white/7 pb-5 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <p className="font-mono text-[9px] uppercase tracking-[0.22em] text-white/22">
              {mode === "population" ? "A / Analytical Field" : "O / Opportunity Intelligence"}
            </p>
            <h1 className="mt-2 text-2xl font-semibold tracking-[-0.035em] text-white/82 sm:text-3xl">
              {mode === "population"
                ? "One population. Coordinated representations."
                : "Opportunity as evidence, not a single score."}
            </h1>
            <p className="mt-2 max-w-3xl text-[11px] leading-5 text-white/29">
              Lens, Population, Focus, Selection and Compare remain shared with Map,
              Company and Relationship. Local analytical filters run only on the
              bounded loaded subset; canonical business rules remain server-side.
            </p>
          </div>
          <span className="font-mono text-[9px] text-white/22">
            {snapshot.generatedAt ? `served ${snapshot.generatedAt}` : "serving time unavailable"}
          </span>
        </header>

        <SummaryRail
          summary={effectiveSummary}
          mode={mode}
          localFiltered={filters.length > 0}
        />

        {filters.length > 0 ? (
          <div className="mb-4 mt-4 flex flex-wrap items-center gap-1.5">
            {filters.map((filter) => (
              <button
                key={`${filter.dimension}:${filter.value}`}
                type="button"
                onClick={() => toggleFilter(filter)}
                className="rounded-full border border-white/9 bg-white/[0.035] px-2.5 py-1.5 text-[8px] text-white/42 hover:bg-white/[0.07] hover:text-white"
              >
                {filter.label} ×
              </button>
            ))}
            {derivedQuery.isFetching ? (
              <span className="ml-1 inline-flex items-center gap-1.5 text-[8px] uppercase tracking-[0.12em] text-white/20">
                <LoaderCircle className="size-3 animate-spin" />
                recomputing loaded subset
              </span>
            ) : null}
          </div>
        ) : null}

        {query.isPending ? (
          <AnalyticsState icon={LoaderCircle} title="Loading analytical population" spinning />
        ) : query.isError ? (
          <AnalyticsState
            icon={AlertTriangle}
            title="Analytics serving request failed"
            detail={query.error instanceof Error ? query.error.message : undefined}
          />
        ) : snapshot.availability === "unavailable" ? (
          <AnalyticsState
            icon={Database}
            title="Analytics serving capability unavailable"
            detail={snapshot.message}
          />
        ) : (
          <div className="space-y-4">
            {mode === "population" ? (
              <>
                <AnalyticsDistributions
                  distributions={effectiveDistributions}
                  filters={filters}
                  onFilter={toggleFilter}
                  onPopulation={activatePopulation}
                />
                <AnalyticsTrajectory series={snapshot.series} />
              </>
            ) : (
              <OpportunityField
                companies={filteredCompanies}
                products={snapshot.products}
                territories={snapshot.territories}
                onFocusCompany={focusCompany}
                onFilter={toggleFilter}
                onPopulation={activatePopulation}
              />
            )}

            <AnalyticsCompare
              compare={compare}
              companies={snapshot.companies}
              onOpenCompany={openCompany}
            />

            {candidateRailOpen ? (
              <AnalyticsCandidates
                mode={mode}
                companies={filteredCompanies}
                selectedCompanyIds={selectedCompanyIds}
                comparedCompanyIds={comparedCompanyIds}
                onFocus={focusCompany}
                onSelection={(company) => toggleSelection(companyEntity(company))}
                onCompare={(company) => toggleCompare(companyEntity(company))}
                onOpenCompany={openCompany}
              />
            ) : null}

            <button
              type="button"
              onClick={() => setCandidateRailOpen((open) => !open)}
              className="text-[8px] uppercase tracking-[0.14em] text-white/18 hover:text-white/45"
            >
              {candidateRailOpen ? "Collapse exact company field" : "Expand exact company field"}
            </button>
          </div>
        )}
      </div>
    </main>
  )
}

function AnalyticsControlDeck({
  mode,
  endpointConfigured,
  population,
  servedPopulation,
  filters,
  loadedCount,
  computing,
  handoffBlocked,
  onModeChange,
  onClearFilters,
  onPopulation,
  onHandoff,
}: {
  mode: AnalyticsMode
  endpointConfigured: boolean
  population: PopulationRef | null
  servedPopulation: PopulationRef | null
  filters: AnalyticsFilter[]
  loadedCount: number
  computing: boolean
  handoffBlocked: boolean
  onModeChange: (mode: AnalyticsMode) => void
  onClearFilters: () => void
  onPopulation: (population: PopulationRef) => void
  onHandoff: (surface: "M" | "R") => void
}) {
  return (
    <aside className="pointer-events-auto fixed left-4 right-[4.75rem] top-4 z-[45] sm:left-7 sm:right-auto sm:top-7 sm:w-[430px]">
      <div className="rounded-2xl border border-white/10 bg-[#081016]/92 p-2 shadow-[0_24px_80px_rgba(0,0,0,.4)] backdrop-blur-2xl">
        <div className="flex items-center gap-2">
          <span className="grid size-9 shrink-0 place-items-center rounded-xl bg-white font-mono text-[11px] font-bold text-black">
            {mode === "population" ? "A" : "O"}
          </span>
          <div className="min-w-0 flex-1">
            <p className="truncate text-[10px] font-semibold text-white/60">
              {mode === "population" ? "Population Analytical Field" : "Opportunity Intelligence"}
            </p>
            <p className="mt-0.5 truncate text-[8px] uppercase tracking-[0.12em] text-white/21">
              {population
                ? `${population.label} · ${population.count.toLocaleString()}`
                : `${loadedCount.toLocaleString()} exact rows loaded`}
            </p>
          </div>
          <span
            className={cn(
              "size-1.5 rounded-full",
              endpointConfigured ? "bg-emerald-300/70" : "bg-white/16"
            )}
          />
        </div>

        <div className="mt-2 grid grid-cols-2 gap-1">
          {(["population", "opportunity"] as const).map((candidate) => (
            <button
              key={candidate}
              type="button"
              onClick={() => onModeChange(candidate)}
              className={cn(
                "rounded-xl border px-3 py-2 text-left text-[9px] transition",
                candidate === mode
                  ? "border-white/18 bg-white text-black"
                  : "border-white/7 bg-white/[0.018] text-white/32 hover:bg-white/[0.05] hover:text-white"
              )}
            >
              {candidate === "population" ? "A · Population" : "O · Opportunity"}
            </button>
          ))}
        </div>

        <div className="mt-2 flex items-center gap-1.5 border-t border-white/7 pt-2">
          {filters.length > 0 ? (
            <button
              type="button"
              onClick={onClearFilters}
              className="inline-flex h-8 items-center gap-1.5 rounded-lg border border-white/7 px-2 text-[8px] text-white/28 hover:bg-white/[0.05] hover:text-white"
            >
              <FilterX className="size-3" />
              Clear {filters.length}
            </button>
          ) : null}

          {servedPopulation &&
          servedPopulation.key !== population?.key ? (
            <button
              type="button"
              onClick={() => onPopulation(servedPopulation)}
              className="h-8 rounded-lg border border-emerald-200/10 bg-emerald-200/[0.035] px-2 text-[8px] text-emerald-100/52 hover:bg-emerald-200/[0.07]"
            >
              Activate served population
            </button>
          ) : null}

          {computing ? (
            <LoaderCircle className="ml-1 size-3 animate-spin text-white/22" />
          ) : null}

          <div className="ml-auto flex gap-1">
            <button
              type="button"
              onClick={() => onHandoff("M")}
              disabled={handoffBlocked}
              title={
                handoffBlocked
                  ? `Refine to ≤${MAX_ANALYTICS_GLOBAL_SELECTION} exact companies or activate a served population`
                  : "Project current analytical population to Map"
              }
              className="grid size-8 place-items-center rounded-lg border border-white/7 text-white/28 hover:bg-white/[0.05] hover:text-white disabled:cursor-not-allowed disabled:opacity-20"
            >
              <MapIcon className="size-3.5" />
            </button>
            <button
              type="button"
              onClick={() => onHandoff("R")}
              disabled={handoffBlocked}
              title={
                handoffBlocked
                  ? `Refine to ≤${MAX_ANALYTICS_GLOBAL_SELECTION} exact companies or activate a served population`
                  : "Project current analytical population to Relationship"
              }
              className="grid size-8 place-items-center rounded-lg border border-white/7 text-white/28 hover:bg-white/[0.05] hover:text-white disabled:cursor-not-allowed disabled:opacity-20"
            >
              <Network className="size-3.5" />
            </button>
          </div>
        </div>
      </div>
    </aside>
  )
}

function SummaryRail({
  summary,
  mode,
  localFiltered,
}: {
  summary: AnalyticsSummary
  mode: AnalyticsMode
  localFiltered: boolean
}) {
  const metrics =
    mode === "population"
      ? [
          ["Companies", formatAnalyticsMetric(summary.companyCount, "number", true)],
          ["TIP penetration", formatAnalyticsMetric(summary.tipPenetration, "percent")],
          ["Active", formatAnalyticsMetric(summary.active, "number", true)],
          ["Prospect", formatAnalyticsMetric(summary.prospect, "number", true)],
          ["Total income", formatAnalyticsMetric(summary.totalIncome, "currency", true)],
          ["Median income", formatAnalyticsMetric(summary.medianIncome, "currency", true)],
        ]
      : [
          ["Companies", formatAnalyticsMetric(summary.companyCount, "number", true)],
          ["Prospect", formatAnalyticsMetric(summary.prospect, "number", true)],
          ["Premium", formatAnalyticsMetric(summary.totalPremium, "currency", true)],
          ["Loss", formatAnalyticsMetric(summary.totalLoss, "currency", true)],
          ["Loss ratio", formatAnalyticsMetric(summary.lossRatio, "percent")],
          ["Opportunity", formatOpportunityScore(summary.averageOpportunityScore)],
        ]

  return (
    <section className="mb-4 overflow-hidden rounded-[22px] border border-white/8 bg-[#0a1015]/64 backdrop-blur-xl">
      <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6">
        {metrics.map(([label, value]) => (
          <div
            key={label}
            className="border-b border-r border-white/6 px-4 py-3 last:border-r-0 xl:border-b-0"
          >
            <p className="font-mono text-[13px] text-white/68">{value}</p>
            <p className="mt-1 text-[8px] uppercase tracking-[0.12em] text-white/21">
              {label}
            </p>
          </div>
        ))}
      </div>
      {localFiltered ? (
        <div className="border-t border-white/6 px-4 py-2 text-[8px] uppercase tracking-[0.12em] text-amber-100/32">
          Local cross-filter summary · loaded exact subset only
        </div>
      ) : null}
    </section>
  )
}

function AnalyticsState({
  icon: Icon,
  title,
  detail,
  spinning = false,
}: {
  icon: typeof Activity
  title: string
  detail?: string
  spinning?: boolean
}) {
  return (
    <div className="grid min-h-[520px] place-items-center rounded-[28px] border border-white/8 bg-[#0a1015]/52 px-8 text-center">
      <div>
        <Icon className={cn("mx-auto size-6 text-white/20", spinning && "animate-spin")} />
        <p className="mt-3 text-sm font-medium text-white/48">{title}</p>
        {detail ? (
          <p className="mx-auto mt-2 max-w-xl text-[10px] leading-5 text-white/26">
            {detail}
          </p>
        ) : null}
      </div>
    </div>
  )
}

function companyEntity(company: AnalyticsCompanyRow): EntityRef {
  return {
    id: company.id,
    kind: "company",
    label: company.name,
    secondary:
      [company.businessType, company.province].filter(Boolean).join(" · ") ||
      undefined,
  }
}

function resolveFilteredCompanies(
  companies: AnalyticsCompanyRow[],
  filters: AnalyticsFilter[],
  derived: AnalyticsDerivedSubset | null
) {
  if (filters.length === 0) return companies
  if (!derived) return []

  const ids = new Set(derived.companyIds)
  return companies.filter((company) => ids.has(company.id))
}

function derivedDistributions(
  derived: AnalyticsDerivedSubset
): AnalyticsDistribution[] {
  return derived.distributions.map((distribution) => ({
    id: `local:${distribution.dimension}`,
    label:
      distribution.dimension === "tipStatus"
        ? "TIP status"
        : distribution.dimension === "businessType"
          ? "Business type"
          : distribution.dimension === "incomeBand"
            ? "Income band"
            : "Province",
    format: "number",
    buckets: distribution.buckets.map((bucket) => ({
      id: `${distribution.dimension}:${bucket.value}`,
      label: bucket.label,
      count: bucket.count,
      filter: {
        dimension: distribution.dimension,
        value: bucket.value,
        label: bucket.label,
      },
    })),
  }))
}
