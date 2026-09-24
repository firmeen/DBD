"use client"

import { BarChart3, Building, TrendingUp } from "lucide-react"

import { formatCompanyMetric, formatSignedChange } from "@/lib/company/format"
import type { CompanyIndustrySnapshot } from "@/lib/company/types"

export function CompanyPosition({
  industry,
}: {
  industry: CompanyIndustrySnapshot | null
}) {
  if (!industry) {
    return (
      <PositionEmpty message="Industry benchmark capability is unavailable for this company." />
    )
  }

  const usable = industry.metrics.filter(
    (metric) =>
      metric.companyValue !== null && metric.industryAverage !== null
  )

  return (
    <div className="space-y-5">
      <section className="rounded-[26px] border border-tipx-line/8 bg-tipx-surface/72 p-6 backdrop-blur-2xl">
        <div className="grid gap-5 lg:grid-cols-[1fr_auto] lg:items-end">
          <div>
            <p className="flex items-center gap-2 text-[9px] font-semibold uppercase tracking-[0.2em] text-tipx-ink/27">
              <BarChart3 className="size-3.5" />
              Industry position · FY {industry.fiscalYear}
            </p>
            <h2 className="mt-2 text-2xl font-semibold tracking-[-0.035em] text-tipx-ink">
              Company versus its operating field
            </h2>
            <p className="mt-2 max-w-2xl text-[11px] leading-5 text-tipx-ink/31">
              Precision comparison strips preserve magnitude and direction
              without compressing unrelated metrics into gauges or radar shapes.
            </p>
          </div>

          <div className="grid grid-cols-3 gap-1.5">
            <PopulationMetric
              label="Submitted"
              value={industry.submitFinancialCount}
            />
            <PopulationMetric label="Profit" value={industry.profitCount} />
            <PopulationMetric label="Loss" value={industry.lossCount} />
          </div>
        </div>
      </section>

      <section className="rounded-[26px] border border-tipx-line/8 bg-tipx-ink/[0.018] p-5 sm:p-6">
        {usable.length ? (
          <div className="divide-y divide-tipx-line/7">
            {usable.map((metric) => (
              <BenchmarkStrip key={metric.code} metric={metric} />
            ))}
          </div>
        ) : (
          <div className="grid min-h-[360px] place-items-center text-center">
            <div>
              <Building className="mx-auto size-5 text-tipx-ink/18" />
              <p className="mt-3 text-xs text-tipx-ink/40">
                No comparable benchmark metrics available.
              </p>
            </div>
          </div>
        )}
      </section>
    </div>
  )
}

function BenchmarkStrip({
  metric,
}: {
  metric: CompanyIndustrySnapshot["metrics"][number]
}) {
  const company = metric.companyValue
  const average = metric.industryAverage
  const previous = metric.industryAveragePrevious
  const values = [company, average, previous].filter(
    (value): value is number => value !== null && Number.isFinite(value)
  )
  const min = values.length ? Math.min(...values) : 0
  const max = values.length ? Math.max(...values) : 1

  const position = (value: number | null) => {
    if (value === null) return null
    if (max === min) return 50
    return ((value - min) / (max - min)) * 100
  }

  const companyPosition = position(company)
  const averagePosition = position(average)
  const previousPosition = position(previous)

  return (
    <div className="py-5 first:pt-0 last:pb-0">
      <div className="flex items-start justify-between gap-4">
        <div>
          <p className="text-[11px] font-medium text-tipx-ink/61">
            {metric.label}
          </p>
          <p className="mt-0.5 font-mono text-[8px] uppercase tracking-[0.12em] text-tipx-ink/18">
            {metric.code}
          </p>
        </div>
        <span className="inline-flex items-center gap-1.5 font-mono text-[9px] text-tipx-ink/30">
          <TrendingUp className="size-3" />
          Industry Δ {formatSignedChange(metric.pctChange)}
        </span>
      </div>

      <div className="mt-5">
        <div className="relative h-8">
          <div className="absolute left-0 right-0 top-1/2 h-px -translate-y-1/2 bg-tipx-ink/9" />
          {previousPosition !== null ? (
            <span
              className="absolute top-1/2 size-2 -translate-x-1/2 -translate-y-1/2 rounded-full border border-tipx-line/16 bg-[#26313a]"
              style={{ left: `${previousPosition}%` }}
              title="Previous industry average"
            />
          ) : null}
          {averagePosition !== null ? (
            <span
              className="absolute top-1/2 size-3 -translate-x-1/2 -translate-y-1/2 rounded-full border border-cyan-100/40 bg-cyan-100/55 shadow-[0_0_22px_rgba(165,243,252,.16)]"
              style={{ left: `${averagePosition}%` }}
              title="Industry average"
            />
          ) : null}
          {companyPosition !== null ? (
            <span
              className="absolute top-1/2 size-4 -translate-x-1/2 -translate-y-1/2 rounded-full border-2 border-black bg-tipx-brand shadow-[0_0_0_1px_rgba(255,255,255,.18),0_8px_24px_rgba(0,0,0,.4)]"
              style={{ left: `${companyPosition}%` }}
              title="Company"
            />
          ) : null}
        </div>

        <div className="grid grid-cols-3 gap-3 text-[9px]">
          <MetricLegend
            label="Company"
            value={formatCompanyMetric(company, metric.format, true)}
            emphasized
          />
          <MetricLegend
            label="Industry avg"
            value={formatCompanyMetric(average, metric.format, true)}
          />
          <MetricLegend
            label="Prior avg"
            value={formatCompanyMetric(previous, metric.format, true)}
          />
        </div>
      </div>
    </div>
  )
}

function MetricLegend({
  label,
  value,
  emphasized = false,
}: {
  label: string
  value: string
  emphasized?: boolean
}) {
  return (
    <div>
      <p className="uppercase tracking-[0.12em] text-tipx-ink/21">{label}</p>
      <p
        className={
          emphasized
            ? "mt-1 font-mono text-[11px] text-tipx-ink/78"
            : "mt-1 font-mono text-[10px] text-tipx-ink/42"
        }
      >
        {value}
      </p>
    </div>
  )
}

function PopulationMetric({
  label,
  value,
}: {
  label: string
  value: number | null
}) {
  return (
    <div className="min-w-20 rounded-xl border border-tipx-line/8 bg-tipx-ink/[0.025] px-3 py-2 text-right">
      <p className="font-mono text-[12px] text-tipx-ink/67">
        {value === null ? "—" : value.toLocaleString()}
      </p>
      <p className="mt-0.5 text-[8px] uppercase tracking-[0.12em] text-tipx-ink/22">
        {label}
      </p>
    </div>
  )
}

function PositionEmpty({ message }: { message: string }) {
  return (
    <div className="grid min-h-[460px] place-items-center rounded-[26px] border border-tipx-line/8 bg-tipx-ink/[0.018] px-6 text-center">
      <div>
        <Building className="mx-auto size-6 text-tipx-ink/18" />
        <p className="mt-3 text-sm font-medium text-tipx-ink/52">
          Industry position unavailable
        </p>
        <p className="mx-auto mt-2 max-w-md text-[11px] leading-5 text-tipx-ink/28">
          {message}
        </p>
      </div>
    </div>
  )
}
