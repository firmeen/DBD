"use client"

import * as React from "react"
import { useVirtualizer } from "@tanstack/react-virtual"
import type { EChartsOption } from "echarts"
import ReactECharts from "echarts-for-react"
import { Activity, CalendarRange, TableProperties } from "lucide-react"

import {
  formatCompanyDate,
  formatCompanyMetric,
  formatSignedChange,
} from "@/lib/company/format"
import type {
  CompanyFinancialSeries,
  CompanyFinancialYear,
} from "@/lib/company/types"
import { cn } from "@/lib/utils"

export function CompanyFinancial({
  years,
  series,
  selectedYear,
  onYearChange,
}: {
  years: CompanyFinancialYear[]
  series: CompanyFinancialSeries[]
  selectedYear: string | null
  onYearChange: (year: string) => void
}) {
  if (years.length === 0 && series.length === 0) {
    return (
      <FinancialEmpty message="Financial serving capability is unavailable for this company." />
    )
  }

  const year =
    years.find((candidate) => candidate.year === selectedYear) ?? years[0] ?? null

  return (
    <div className="space-y-5">
      <section className="rounded-[26px] border border-tipx-line/8 bg-tipx-surface/72 p-5 backdrop-blur-2xl sm:p-6">
        <div className="flex flex-col gap-5 border-b border-tipx-line/7 pb-5 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <p className="flex items-center gap-2 text-[9px] font-semibold uppercase tracking-[0.2em] text-tipx-ink/27">
              <Activity className="size-3.5" />
              Financial trajectory
            </p>
            <h2 className="mt-2 text-xl font-semibold tracking-[-0.03em] text-tipx-ink">
              Fiscal-year movement, not a KPI collage
            </h2>
            <p className="mt-1 max-w-2xl text-[11px] leading-5 text-tipx-ink/32">
              Semantic series arrive from the Center Database serving model; the
              browser does not infer raw DBD item codes.
            </p>
          </div>

          <YearSelector
            years={years}
            value={year?.year ?? null}
            onChange={onYearChange}
          />
        </div>

        <div className="mt-5">
          <FinancialTrajectoryCharts series={series} />
        </div>
      </section>

      <div className="grid gap-5 xl:grid-cols-[.78fr_1.22fr]">
        <RatioPanel year={year} />
        <FinancialStatement year={year} />
      </div>
    </div>
  )
}

function FinancialTrajectoryCharts({
  series,
}: {
  series: CompanyFinancialSeries[]
}) {
  const grouped = React.useMemo(() => {
    const groups = new Map<
      CompanyFinancialSeries["format"],
      CompanyFinancialSeries[]
    >()

    for (const metric of series) {
      const current = groups.get(metric.format) ?? []
      current.push(metric)
      groups.set(metric.format, current)
    }

    return [...groups.entries()]
  }, [series])

  if (grouped.length === 0) {
    return (
      <div className="grid h-[300px] place-items-center text-center">
        <div>
          <Activity className="mx-auto size-5 text-tipx-ink/18" />
          <p className="mt-3 text-xs text-tipx-ink/42">
            No trajectory series available
          </p>
        </div>
      </div>
    )
  }

  return (
    <div className="grid gap-4 xl:grid-cols-2">
      {grouped.map(([format, metrics]) => (
        <TrajectoryField key={format} format={format} series={metrics} />
      ))}
    </div>
  )
}

function TrajectoryField({
  format,
  series,
}: {
  format: CompanyFinancialSeries["format"]
  series: CompanyFinancialSeries[]
}) {
  const option = React.useMemo<EChartsOption>(() => {
    const categories = Array.from(
      new Set(series.flatMap((metric) => metric.points.map((point) => point.year)))
    ).sort()

    return {
      animationDuration: 420,
      animationEasing: "cubicOut",
      backgroundColor: "transparent",
      grid: {
        left: 12,
        right: 18,
        top: 42,
        bottom: 24,
        containLabel: true,
      },
      legend: {
        type: "scroll",
        top: 0,
        left: 0,
        textStyle: {
          color: "rgba(255,255,255,.48)",
          fontSize: 9,
        },
        pageTextStyle: {
          color: "rgba(255,255,255,.34)",
        },
      },
      tooltip: {
        trigger: "axis",
        backgroundColor: "rgba(8,16,22,.96)",
        borderColor: "rgba(255,255,255,.12)",
        textStyle: {
          color: "rgba(255,255,255,.78)",
          fontSize: 10,
        },
      },
      xAxis: {
        type: "category",
        data: categories,
        boundaryGap: false,
        axisLine: { lineStyle: { color: "rgba(255,255,255,.09)" } },
        axisTick: { show: false },
        axisLabel: { color: "rgba(255,255,255,.28)", fontSize: 9 },
      },
      yAxis: {
        type: "value",
        splitLine: { lineStyle: { color: "rgba(255,255,255,.055)" } },
        axisLabel: {
          color: "rgba(255,255,255,.26)",
          fontSize: 9,
          formatter: (value: number | string) =>
            formatCompanyMetric(Number(value), format, true),
        },
      },
      series: series.map((metric, index) => ({
        name: metric.label,
        type: "line",
        smooth: 0.24,
        showSymbol: false,
        symbolSize: 6,
        lineStyle: {
          width: index === 0 ? 2.4 : 1.5,
          opacity: index === 0 ? 0.94 : 0.58,
        },
        emphasis: {
          focus: "series",
          lineStyle: { width: 2.8, opacity: 1 },
        },
        data: categories.map((year) => {
          const point = metric.points.find((candidate) => candidate.year === year)
          return point?.value ?? null
        }),
      })),
    }
  }, [format, series])

  return (
    <div className="rounded-2xl border border-tipx-line/7 bg-tipx-ink/[0.025] p-3">
      <div className="mb-2 flex items-center justify-between px-1">
        <p className="text-[8px] font-semibold uppercase tracking-[0.17em] text-tipx-ink/24">
          {trajectoryFormatLabel(format)}
        </p>
        <span className="font-mono text-[8px] text-tipx-ink/17">
          {series.length} series
        </span>
      </div>
      <div
        aria-label={`${trajectoryFormatLabel(format)} financial trajectory`}
        className="h-[280px]"
      >
        <ReactECharts
          option={option}
          notMerge
          lazyUpdate
          style={{ height: "100%", width: "100%" }}
        />
      </div>
    </div>
  )
}

function trajectoryFormatLabel(format: CompanyFinancialSeries["format"]) {
  switch (format) {
    case "currency":
      return "Currency field"
    case "percent":
      return "Percentage field"
    case "ratio":
      return "Ratio field"
    default:
      return "Numeric field"
  }
}

function YearSelector({
  years,
  value,
  onChange,
}: {
  years: CompanyFinancialYear[]
  value: string | null
  onChange: (year: string) => void
}) {
  if (years.length === 0) return null

  return (
    <div className="flex max-w-full items-center gap-1 overflow-x-auto rounded-xl border border-tipx-line/8 bg-tipx-ink/5 p-1">
      {years.map((year) => (
        <button
          key={year.year}
          type="button"
          onClick={() => onChange(year.year)}
          className={cn(
            "shrink-0 rounded-lg px-3 py-2 font-mono text-[9px] transition",
            value === year.year
              ? "bg-tipx-brand text-tipx-on-brand"
              : "text-tipx-ink/35 hover:bg-tipx-ink/6 hover:text-tipx-ink/65"
          )}
        >
          {year.year}
        </button>
      ))}
    </div>
  )
}

function RatioPanel({ year }: { year: CompanyFinancialYear | null }) {
  return (
    <section className="rounded-[24px] border border-tipx-line/8 bg-tipx-ink/[0.018] p-5 sm:p-6">
      <p className="flex items-center gap-2 text-[9px] font-semibold uppercase tracking-[0.2em] text-tipx-ink/27">
        <CalendarRange className="size-3.5" />
        Ratio field · {year?.year ?? "—"}
      </p>
      <p className="mt-2 text-[11px] leading-5 text-tipx-ink/31">
        Ratios stay separate for accurate reading instead of being compressed
        into a decorative radar chart.
      </p>

      <div className="mt-5 divide-y divide-tipx-line/7">
        {year?.ratios.length ? (
          year.ratios.map((ratio) => (
            <div
              key={ratio.code}
              className="grid grid-cols-[1fr_auto] items-center gap-4 py-3 first:pt-0"
            >
              <span>
                <span className="block text-[11px] text-tipx-ink/57">
                  {ratio.label}
                </span>
                <span className="mt-0.5 block font-mono text-[8px] uppercase tracking-[0.12em] text-tipx-ink/18">
                  {ratio.code}
                </span>
              </span>
              <span className="font-mono text-[12px] text-tipx-ink/76">
                {formatCompanyMetric(ratio.value, ratio.format)}
              </span>
            </div>
          ))
        ) : (
          <p className="py-8 text-center text-[11px] text-tipx-ink/28">
            No ratio series for this fiscal year.
          </p>
        )}
      </div>
    </section>
  )
}

function FinancialStatement({
  year,
}: {
  year: CompanyFinancialYear | null
}) {
  const scrollRef = React.useRef<HTMLDivElement>(null)
  const items = year?.lineItems ?? []
  const virtualizer = useVirtualizer({
    count: items.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => 54,
    overscan: 10,
  })

  return (
    <section className="overflow-hidden rounded-[24px] border border-tipx-line/8 bg-tipx-ink/[0.018]">
      <div className="flex items-end justify-between gap-4 border-b border-tipx-line/7 px-5 py-5 sm:px-6">
        <div>
          <p className="flex items-center gap-2 text-[9px] font-semibold uppercase tracking-[0.2em] text-tipx-ink/27">
            <TableProperties className="size-3.5" />
            Statement explorer
          </p>
          <p className="mt-1.5 text-[11px] text-tipx-ink/31">
            {year?.fiscalYearEndDate
              ? `Year end ${formatCompanyDate(year.fiscalYearEndDate)}`
              : "Virtualized semantic line items"}
          </p>
        </div>
        <span className="font-mono text-[9px] text-tipx-ink/22">
          {items.length.toLocaleString()} lines
        </span>
      </div>

      <div className="grid grid-cols-[minmax(160px,1fr)_110px_110px_74px] border-b border-tipx-line/7 bg-tipx-ink/[0.025] px-5 py-2 text-[8px] uppercase tracking-[0.12em] text-tipx-ink/22 sm:px-6">
        <span>Line item</span>
        <span className="text-right">Current</span>
        <span className="text-right">Previous</span>
        <span className="text-right">Change</span>
      </div>

      <div ref={scrollRef} className="h-[420px] overflow-auto">
        {items.length === 0 ? (
          <div className="grid h-full place-items-center text-[11px] text-tipx-ink/28">
            No statement lines available.
          </div>
        ) : (
          <div
            className="relative w-full"
            style={{ height: virtualizer.getTotalSize() }}
          >
            {virtualizer.getVirtualItems().map((virtualRow) => {
              const item = items[virtualRow.index]
              return (
                <div
                  key={item.code}
                  data-index={virtualRow.index}
                  ref={virtualizer.measureElement}
                  className="absolute left-0 top-0 grid w-full grid-cols-[minmax(160px,1fr)_110px_110px_74px] items-center border-b border-tipx-line/[0.045] px-5 py-3 text-[10px] sm:px-6"
                  style={{
                    transform: `translateY(${virtualRow.start}px)`,
                  }}
                >
                  <span className="min-w-0 pr-3">
                    <span className="block truncate text-tipx-ink/56">
                      {item.label}
                    </span>
                    <span className="mt-0.5 block truncate font-mono text-[8px] text-tipx-ink/17">
                      {item.code}
                    </span>
                  </span>
                  <span className="text-right font-mono text-tipx-ink/64">
                    {formatCompanyMetric(item.amount, "currency", true)}
                  </span>
                  <span className="text-right font-mono text-tipx-ink/34">
                    {formatCompanyMetric(
                      item.previousAmount,
                      "currency",
                      true
                    )}
                  </span>
                  <span
                    className={cn(
                      "text-right font-mono",
                      (item.pctChange ?? 0) > 0
                        ? "text-emerald-200/58"
                        : (item.pctChange ?? 0) < 0
                          ? "text-rose-200/58"
                          : "text-tipx-ink/30"
                    )}
                  >
                    {formatSignedChange(item.pctChange)}
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

function FinancialEmpty({ message }: { message: string }) {
  return (
    <div className="grid min-h-[460px] place-items-center rounded-[26px] border border-tipx-line/8 bg-tipx-ink/[0.018] px-6 text-center">
      <div>
        <Activity className="mx-auto size-6 text-tipx-ink/18" />
        <p className="mt-3 text-sm font-medium text-tipx-ink/52">
          Financial intelligence unavailable
        </p>
        <p className="mx-auto mt-2 max-w-md text-[11px] leading-5 text-tipx-ink/28">
          {message}
        </p>
      </div>
    </div>
  )
}
