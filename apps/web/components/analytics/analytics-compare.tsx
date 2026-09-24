"use client"

import { GitCompare } from "lucide-react"

import { formatAnalyticsMetric, formatOpportunityScore } from "@/lib/analytics/format"
import type { AnalyticsCompanyRow } from "@/lib/analytics/types"
import type { EntityRef } from "@/lib/intelligence/types"

export function AnalyticsCompare({
  compare,
  companies,
  onOpenCompany,
}: {
  compare: EntityRef[]
  companies: AnalyticsCompanyRow[]
  onOpenCompany: (company: AnalyticsCompanyRow) => void
}) {
  const companyById = new Map(companies.map((company) => [company.id, company]))
  const compared = compare
    .filter((entity) => entity.kind === "company")
    .map((entity) => ({
      entity,
      company: companyById.get(entity.id) ?? null,
    }))
    .slice(0, 6)

  if (compared.length === 0) return null

  return (
    <section className="overflow-hidden rounded-[24px] border border-white/8 bg-[#0a1015]/70 backdrop-blur-xl">
      <div className="flex items-center gap-2 border-b border-white/7 px-4 py-3">
        <GitCompare className="size-3.5 text-white/27" />
        <p className="text-[9px] font-semibold uppercase tracking-[0.18em] text-white/28">
          Compare context
        </p>
        <span className="ml-auto font-mono text-[8px] text-white/18">
          {compared.length} retained
        </span>
      </div>
      <div className="overflow-x-auto">
        <div className="grid min-w-[760px] grid-cols-[minmax(220px,1.5fr)_120px_120px_100px_110px] px-4 py-2 text-[8px] uppercase tracking-[0.12em] text-white/18">
          <span>Company</span>
          <span className="text-right">Income</span>
          <span className="text-right">Premium</span>
          <span className="text-right">Loss ratio</span>
          <span className="text-right">Opportunity</span>
        </div>
        {compared.map(({ entity, company }) => (
          <button
            key={entity.id}
            type="button"
            disabled={!company}
            onClick={() => company && onOpenCompany(company)}
            className="grid min-w-[760px] w-full grid-cols-[minmax(220px,1.5fr)_120px_120px_100px_110px] border-t border-white/5 px-4 py-2.5 text-left transition enabled:hover:bg-white/[0.035] disabled:cursor-default"
          >
            <span className="min-w-0">
              <span className="block truncate text-[10px] text-white/53">
                {entity.label}
              </span>
              <span className="mt-0.5 block truncate text-[8px] text-white/19">
                {company
                  ? [company.businessType, company.province].filter(Boolean).join(" · ") || "Loaded company"
                  : "Not in current exact loaded subset"}
              </span>
            </span>
            <span className="text-right font-mono text-[9px] text-white/42">
              {formatAnalyticsMetric(company?.totalIncome, "currency", true)}
            </span>
            <span className="text-right font-mono text-[9px] text-white/38">
              {formatAnalyticsMetric(company?.premium, "currency", true)}
            </span>
            <span className="text-right font-mono text-[9px] text-white/38">
              {formatAnalyticsMetric(company?.lossRatio, "percent")}
            </span>
            <span className="text-right font-mono text-[9px] text-white/55">
              {formatOpportunityScore(company?.opportunityScore)}
            </span>
          </button>
        ))}
      </div>
    </section>
  )
}
