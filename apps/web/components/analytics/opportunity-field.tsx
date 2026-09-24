"use client"

import * as React from "react"
import type { EChartsOption } from "echarts"
import ReactECharts from "echarts-for-react"
import { ArrowUpRight, MapPinned, PackageSearch } from "lucide-react"

import { formatAnalyticsMetric, formatOpportunityScore } from "@/lib/analytics/format"
import type {
  AnalyticsCompanyRow,
  AnalyticsFilter,
  AnalyticsProductPerformance,
  AnalyticsTerritory,
} from "@/lib/analytics/types"
import type { PopulationRef } from "@/lib/intelligence/types"

export function OpportunityField({
  companies,
  products,
  territories,
  onFocusCompany,
  onFilter,
  onPopulation,
}: {
  companies: AnalyticsCompanyRow[]
  products: AnalyticsProductPerformance[]
  territories: AnalyticsTerritory[]
  onFocusCompany: (company: AnalyticsCompanyRow) => void
  onFilter: (filter: AnalyticsFilter) => void
  onPopulation: (population: PopulationRef) => void
}) {
  return (
    <div className="grid gap-4 xl:grid-cols-[1.18fr_.82fr]">
      <OpportunityFrontier
        companies={companies}
        onFocusCompany={onFocusCompany}
      />
      <div className="grid gap-4">
        <ProductPerformance products={products} onFilter={onFilter} />
        <TerritoryField territories={territories} onPopulation={onPopulation} />
      </div>
    </div>
  )
}

function OpportunityFrontier({
  companies,
  onFocusCompany,
}: {
  companies: AnalyticsCompanyRow[]
  onFocusCompany: (company: AnalyticsCompanyRow) => void
}) {
  const points = React.useMemo(
    () =>
      companies
        .filter(
          (company) =>
            company.totalIncome != null && company.opportunityScore != null
        )
        .slice(0, 2_500),
    [companies]
  )

  const option = React.useMemo<EChartsOption>(
    () => ({
      animationDuration: 380,
      grid: { left: 14, right: 18, top: 18, bottom: 20, containLabel: true },
      tooltip: {
        trigger: "item",
        backgroundColor: "rgba(8,16,22,.97)",
        borderColor: "rgba(255,255,255,.1)",
        textStyle: { color: "rgba(255,255,255,.76)", fontSize: 10 },
      },
      xAxis: {
        type: "value",
        name: "Income",
        nameTextStyle: { color: "rgba(255,255,255,.24)", fontSize: 8 },
        splitLine: { lineStyle: { color: "rgba(255,255,255,.045)" } },
        axisLabel: {
          color: "rgba(255,255,255,.22)",
          fontSize: 8,
          formatter: (value: number | string) =>
            formatAnalyticsMetric(Number(value), "currency", true),
        },
      },
      yAxis: {
        type: "value",
        min: 0,
        max: 1,
        name: "Opportunity",
        nameTextStyle: { color: "rgba(255,255,255,.24)", fontSize: 8 },
        splitLine: { lineStyle: { color: "rgba(255,255,255,.045)" } },
        axisLabel: {
          color: "rgba(255,255,255,.22)",
          fontSize: 8,
          formatter: (value: number | string) =>
            formatOpportunityScore(Number(value)),
        },
      },
      series: [
        {
          type: "scatter",
          symbolSize: 9,
          itemStyle: { color: "rgba(119,219,194,.62)" },
          emphasis: {
            itemStyle: { color: "rgba(255,255,255,.95)" },
            scale: 1.6,
          },
          data: points.map((company) => [
            company.totalIncome,
            company.opportunityScore,
          ]),
        },
      ],
    }),
    [points]
  )

  const onEvents = React.useMemo(
    () => ({
      click: (params: { dataIndex?: number }) => {
        const company =
          typeof params.dataIndex === "number"
            ? points[params.dataIndex]
            : undefined
        if (company) onFocusCompany(company)
      },
    }),
    [onFocusCompany, points]
  )

  return (
    <section className="rounded-[26px] border border-tipx-line/8 bg-tipx-surface/72 p-5 backdrop-blur-xl">
      <p className="text-[9px] font-semibold uppercase tracking-[0.19em] text-tipx-ink/28">
        Opportunity frontier
      </p>
      <p className="mt-1 text-[10px] text-tipx-ink/23">
        Financial capacity × serving-defined opportunity signal · click to Focus
      </p>
      <ReactECharts
        option={option}
        onEvents={onEvents}
        notMerge
        lazyUpdate
        style={{ height: 410, width: "100%" }}
      />
    </section>
  )
}

function ProductPerformance({
  products,
  onFilter,
}: {
  products: AnalyticsProductPerformance[]
  onFilter: (filter: AnalyticsFilter) => void
}) {
  const rows = [...products]
    .sort((left, right) => right.opportunityCount - left.opportunityCount)
    .slice(0, 8)

  return (
    <section className="rounded-[24px] border border-tipx-line/8 bg-tipx-surface/72 p-4 backdrop-blur-xl">
      <div className="flex items-center gap-2">
        <PackageSearch className="size-3.5 text-tipx-ink/28" />
        <p className="text-[9px] font-semibold uppercase tracking-[0.18em] text-tipx-ink/28">
          Product / subclass field
        </p>
      </div>
      <div className="mt-3 divide-y divide-tipx-line/6">
        {rows.length ? (
          rows.map((row) => (
            <button
              key={row.id}
              type="button"
              onClick={() =>
                onFilter(
                  row.subclass
                    ? {
                        dimension: "productSubclass",
                        value: JSON.stringify([row.product, row.subclass]),
                        product: row.product,
                        subclass: row.subclass,
                        label: `${row.product} · ${row.subclass}`,
                      }
                    : {
                        dimension: "product",
                        value: row.product,
                        label: row.product,
                      }
                )
              }
              className="grid w-full grid-cols-[1fr_auto] gap-3 py-2.5 text-left hover:bg-tipx-ink/[0.02]"
            >
              <span className="min-w-0">
                <span className="block truncate text-[10px] text-tipx-ink/52">
                  {row.product}
                </span>
                <span className="mt-0.5 block truncate text-[8px] text-tipx-ink/21">
                  {row.subclass || "All subclasses"} · {row.companyCount.toLocaleString()} companies
                </span>
              </span>
              <span className="text-right">
                <span className="block font-mono text-[9px] text-tipx-ink/54">
                  {row.opportunityCount.toLocaleString()} opp.
                </span>
                <span className="mt-0.5 block font-mono text-[8px] text-tipx-ink/23">
                  SI {formatAnalyticsMetric(row.sumInsured, "currency", true)} ·
                  P {formatAnalyticsMetric(row.premium, "currency", true)} ·
                  LR {formatAnalyticsMetric(row.lossRatio, "percent")}
                </span>
              </span>
            </button>
          ))
        ) : (
          <p className="py-8 text-center text-[10px] text-tipx-ink/22">
            Product performance unavailable
          </p>
        )}
      </div>
    </section>
  )
}

function TerritoryField({
  territories,
  onPopulation,
}: {
  territories: AnalyticsTerritory[]
  onPopulation: (population: PopulationRef) => void
}) {
  const rows = [...territories]
    .sort(
      (left, right) =>
        (right.opportunityScore ?? -1) - (left.opportunityScore ?? -1)
    )
    .slice(0, 8)

  return (
    <section className="rounded-[24px] border border-tipx-line/8 bg-tipx-surface/72 p-4 backdrop-blur-xl">
      <div className="flex items-center gap-2">
        <MapPinned className="size-3.5 text-tipx-ink/28" />
        <p className="text-[9px] font-semibold uppercase tracking-[0.18em] text-tipx-ink/28">
          Territory opportunity
        </p>
      </div>
      <div className="mt-3 space-y-1.5">
        {rows.length ? (
          rows.map((row) => (
            <button
              key={row.id}
              type="button"
              disabled={!row.population}
              onClick={() => row.population && onPopulation(row.population)}
              className="flex w-full items-center gap-3 rounded-xl border border-tipx-line/6 bg-tipx-ink/[0.015] px-3 py-2.5 text-left transition enabled:hover:bg-tipx-ink/[0.05] disabled:cursor-default"
            >
              <span className="min-w-0 flex-1">
                <span className="block truncate text-[10px] text-tipx-ink/50">
                  {row.label}
                </span>
                <span className="mt-0.5 block text-[8px] uppercase tracking-[0.1em] text-tipx-ink/19">
                  {row.kind} · {row.prospectCount.toLocaleString()} prospects
                </span>
              </span>
              <span className="font-mono text-[9px] text-tipx-ink/55">
                {formatOpportunityScore(row.opportunityScore)}
              </span>
              {row.population ? (
                <ArrowUpRight className="size-3 text-tipx-ink/23" />
              ) : null}
            </button>
          ))
        ) : (
          <p className="py-8 text-center text-[10px] text-tipx-ink/22">
            Territory analytics unavailable
          </p>
        )}
      </div>
    </section>
  )
}
