"use client"

import { Flag, Globe2 } from "lucide-react"

import { formatCompanyMetric } from "@/lib/company/format"
import { normalizeOwnershipComposition } from "@/lib/company/ownership"
import type {
  CompanyNation,
  CompanyNationHistory,
} from "@/lib/company/types"

export function CompanyOwnership({
  current,
  history,
}: {
  current: CompanyNation[]
  history: CompanyNationHistory[]
}) {
  if (current.length === 0 && history.length === 0) {
    return (
      <OwnershipEmpty message="Ownership/nationality serving capability is unavailable for this company." />
    )
  }

  const ranked = [...current].sort(
    (left, right) => (right.sharePct ?? -1) - (left.sharePct ?? -1)
  )
  const chronological = [...history].sort((left, right) =>
    left.fiscalYear.localeCompare(right.fiscalYear)
  )

  return (
    <div className="grid gap-5 xl:grid-cols-[.78fr_1.22fr]">
      <section className="rounded-[26px] border border-white/8 bg-[#0a1015]/72 p-6 backdrop-blur-2xl">
        <p className="flex items-center gap-2 text-[9px] font-semibold uppercase tracking-[0.2em] text-white/27">
          <Flag className="size-3.5" />
          Current ownership composition
        </p>
        <p className="mt-2 text-[11px] leading-5 text-white/31">
          Ranked composition exposes relative ownership directly rather than
          splitting the same population across multiple pie charts.
        </p>

        <div className="mt-6 space-y-4">
          {ranked.map((nation) => (
            <NationRow key={nation.code} nation={nation} />
          ))}
        </div>
      </section>

      <section className="rounded-[26px] border border-white/8 bg-white/[0.018] p-6">
        <p className="flex items-center gap-2 text-[9px] font-semibold uppercase tracking-[0.2em] text-white/27">
          <Globe2 className="size-3.5" />
          Thai / foreign ownership over time
        </p>
        <p className="mt-2 text-[11px] leading-5 text-white/31">
          A normalized 100% history makes composition shifts comparable across
          fiscal years even when absolute capital changes.
        </p>

        <div className="mt-7 space-y-4">
          {chronological.length ? (
            chronological.map((row) => {
              const composition = normalizeOwnershipComposition(
                row.thaiPct,
                row.otherPct
              )

              return (
                <div key={row.fiscalYear}>
                  <div className="mb-2 flex items-center justify-between">
                    <span className="font-mono text-[10px] text-white/46">
                      {row.fiscalYear}
                    </span>
                    <span className="font-mono text-[9px] text-white/24">
                      TH {formatCompanyMetric(row.thaiPct, "percent")} · OTHER{" "}
                      {formatCompanyMetric(row.otherPct, "percent")}
                    </span>
                  </div>
                  {composition ? (
                    <div className="flex h-3 overflow-hidden rounded-full border border-white/7 bg-white/[0.025]">
                      <span
                        className="h-full bg-white/74"
                        style={{ width: `${composition.thaiWidth}%` }}
                      />
                      <span
                        className="h-full bg-cyan-200/24"
                        style={{ width: `${composition.otherWidth}%` }}
                      />
                    </div>
                  ) : (
                    <div className="flex h-8 items-center rounded-xl border border-dashed border-white/8 bg-white/[0.015] px-3 text-[9px] text-white/24">
                      Composition incomplete · no normalized bar rendered
                    </div>
                  )}
                </div>
              )
            })
          ) : (
            <p className="py-16 text-center text-[11px] text-white/28">
              Historical nationality composition unavailable.
            </p>
          )}
        </div>

        <div className="mt-6 flex items-center gap-5 border-t border-white/7 pt-4 text-[9px] uppercase tracking-[0.12em] text-white/24">
          <span className="inline-flex items-center gap-2">
            <span className="size-2 rounded-full bg-white/74" />
            Thailand
          </span>
          <span className="inline-flex items-center gap-2">
            <span className="size-2 rounded-full bg-cyan-200/24" />
            Other
          </span>
        </div>
      </section>
    </div>
  )
}

function NationRow({ nation }: { nation: CompanyNation }) {
  const share = clampPercent(nation.sharePct)

  return (
    <div>
      <div className="flex items-end justify-between gap-3">
        <span className="min-w-0">
          <span className="block truncate text-[11px] text-white/58">
            {nation.country}
          </span>
          {nation.countryEn ? (
            <span className="mt-0.5 block truncate text-[9px] text-white/22">
              {nation.countryEn}
            </span>
          ) : null}
        </span>
        <span className="shrink-0 font-mono text-[11px] text-white/70">
          {formatCompanyMetric(nation.sharePct, "percent")}
        </span>
      </div>
      <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-white/[0.045]">
        <div
          className="h-full rounded-full bg-white/58"
          style={{ width: `${share}%` }}
        />
      </div>
    </div>
  )
}

function clampPercent(value: number | null | undefined) {
  if (value === null || value === undefined || !Number.isFinite(value)) return 0
  return Math.max(0, Math.min(100, value))
}

function OwnershipEmpty({ message }: { message: string }) {
  return (
    <div className="grid min-h-[460px] place-items-center rounded-[26px] border border-white/8 bg-white/[0.018] px-6 text-center">
      <div>
        <Globe2 className="mx-auto size-6 text-white/18" />
        <p className="mt-3 text-sm font-medium text-white/52">
          Ownership intelligence unavailable
        </p>
        <p className="mx-auto mt-2 max-w-md text-[11px] leading-5 text-white/28">
          {message}
        </p>
      </div>
    </div>
  )
}
