"use client"

import * as React from "react"
import { useVirtualizer } from "@tanstack/react-virtual"
import { Building2, GitCompare, MapPin, Target } from "lucide-react"

import { formatAnalyticsMetric, formatOpportunityScore } from "@/lib/analytics/format"
import type { AnalyticsCompanyRow, AnalyticsMode } from "@/lib/analytics/types"
import { cn } from "@/lib/utils"

export function AnalyticsCandidates({
  mode,
  companies,
  selectedCompanyIds,
  comparedCompanyIds,
  onFocus,
  onSelection,
  onCompare,
  onOpenCompany,
}: {
  mode: AnalyticsMode
  companies: AnalyticsCompanyRow[]
  selectedCompanyIds: ReadonlySet<string>
  comparedCompanyIds: ReadonlySet<string>
  onFocus: (company: AnalyticsCompanyRow) => void
  onSelection: (company: AnalyticsCompanyRow) => void
  onCompare: (company: AnalyticsCompanyRow) => void
  onOpenCompany: (company: AnalyticsCompanyRow) => void
}) {
  const scrollRef = React.useRef<HTMLDivElement>(null)
  const ordered = React.useMemo(
    () =>
      [...companies].sort((left, right) =>
        mode === "opportunity"
          ? (right.opportunityScore ?? -1) - (left.opportunityScore ?? -1)
          : (right.totalIncome ?? Number.NEGATIVE_INFINITY) -
            (left.totalIncome ?? Number.NEGATIVE_INFINITY)
      ),
    [companies, mode]
  )

  const virtualizer = useVirtualizer({
    count: ordered.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => 62,
    overscan: 10,
  })

  return (
    <section className="overflow-hidden rounded-[26px] border border-white/8 bg-[#0a1015]/72 backdrop-blur-xl">
      <div className="flex items-end justify-between border-b border-white/7 px-5 py-4">
        <div>
          <p className="text-[9px] font-semibold uppercase tracking-[0.19em] text-white/28">
            Exact loaded companies
          </p>
          <p className="mt-1 text-[10px] text-white/24">
            Virtualized entity field · Focus / Select / Compare / Open Company
          </p>
        </div>
        <span className="font-mono text-[9px] text-white/24">
          {ordered.length.toLocaleString()}
        </span>
      </div>

      <div className="grid grid-cols-[minmax(220px,1.5fr)_90px_100px_90px_74px_94px] border-b border-white/7 bg-black/10 px-4 py-2 text-[8px] uppercase tracking-[0.12em] text-white/20">
        <span>Company</span>
        <span>TIP</span>
        <span className="text-right">Income</span>
        <span className="text-right">Premium</span>
        <span className="text-right">LR</span>
        <span className="text-right">Opportunity</span>
      </div>

      <div ref={scrollRef} className="h-[420px] overflow-auto">
        {ordered.length === 0 ? (
          <div className="grid h-full place-items-center text-[10px] text-white/24">
            No exact company rows in the current loaded population.
          </div>
        ) : (
          <div
            className="relative w-full"
            style={{ height: virtualizer.getTotalSize() }}
          >
            {virtualizer.getVirtualItems().map((virtualRow) => {
              const company = ordered[virtualRow.index]
              const selected = selectedCompanyIds.has(company.id)
              const compared = comparedCompanyIds.has(company.id)

              return (
                <div
                  key={company.id}
                  data-index={virtualRow.index}
                  ref={virtualizer.measureElement}
                  className="absolute left-0 top-0 grid w-full grid-cols-[minmax(220px,1.5fr)_90px_100px_90px_74px_94px] items-center border-b border-white/[0.045] px-4 py-2.5 text-[9px]"
                  style={{ transform: `translateY(${virtualRow.start}px)` }}
                >
                  <div className="flex min-w-0 items-center gap-2">
                    <button
                      type="button"
                      onClick={() => onFocus(company)}
                      className="grid size-8 shrink-0 place-items-center rounded-lg border border-white/7 bg-white/[0.025] text-white/32 hover:bg-white/[0.06] hover:text-white"
                      aria-label={`Focus ${company.name}`}
                    >
                      <Target className="size-3.5" />
                    </button>
                    <button
                      type="button"
                      onClick={() => onOpenCompany(company)}
                      className="min-w-0 flex-1 text-left"
                    >
                      <span className="block truncate text-[10px] font-medium text-white/58">
                        {company.name}
                      </span>
                      <span className="mt-0.5 flex items-center gap-1 truncate text-[8px] text-white/20">
                        <MapPin className="size-2.5" />
                        {[company.province, company.businessType]
                          .filter(Boolean)
                          .join(" · ") || "Company"}
                      </span>
                    </button>
                    <button
                      type="button"
                      onClick={() => onSelection(company)}
                      className={cn(
                        "grid size-7 shrink-0 place-items-center rounded-lg border transition",
                        selected
                          ? "border-emerald-100/25 bg-emerald-200/10 text-emerald-100/70"
                          : "border-white/7 text-white/20 hover:bg-white/[0.05] hover:text-white"
                      )}
                      title={selected ? "Remove from Selection" : "Add to Selection"}
                    >
                      <Building2 className="size-3" />
                    </button>
                    <button
                      type="button"
                      onClick={() => onCompare(company)}
                      className={cn(
                        "grid size-7 shrink-0 place-items-center rounded-lg border transition",
                        compared
                          ? "border-cyan-100/25 bg-cyan-200/10 text-cyan-100/70"
                          : "border-white/7 text-white/20 hover:bg-white/[0.05] hover:text-white"
                      )}
                      title={compared ? "Remove from Compare" : "Add to Compare"}
                    >
                      <GitCompare className="size-3" />
                    </button>
                  </div>
                  <span className="uppercase text-white/34">
                    {company.tipStatus ?? "—"}
                  </span>
                  <span className="text-right font-mono text-white/48">
                    {formatAnalyticsMetric(company.totalIncome, "currency", true)}
                  </span>
                  <span className="text-right font-mono text-white/42">
                    {formatAnalyticsMetric(company.premium, "currency", true)}
                  </span>
                  <span className="text-right font-mono text-white/40">
                    {formatAnalyticsMetric(company.lossRatio, "percent")}
                  </span>
                  <span className="text-right font-mono text-white/62">
                    {formatOpportunityScore(company.opportunityScore)}
                  </span>
                </div>
              )
            })}
          </div>
        )}
      </div>
    </section>
  )
}
