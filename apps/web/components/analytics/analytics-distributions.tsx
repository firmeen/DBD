"use client"

import * as React from "react"
import type { EChartsOption } from "echarts"
import ReactECharts from "echarts-for-react"

import type {
  AnalyticsBucket,
  AnalyticsDistribution,
  AnalyticsFilter,
} from "@/lib/analytics/types"
import type { PopulationRef } from "@/lib/intelligence/types"
import { cn } from "@/lib/utils"

export function AnalyticsDistributions({
  distributions,
  filters,
  onFilter,
  onPopulation,
}: {
  distributions: AnalyticsDistribution[]
  filters: AnalyticsFilter[]
  onFilter: (filter: AnalyticsFilter) => void
  onPopulation: (population: PopulationRef) => void
}) {
  if (distributions.length === 0) {
    return (
      <div className="grid min-h-[360px] place-items-center rounded-[26px] border border-tipx-line/8 bg-tipx-ink/[0.018] text-center">
        <div>
          <p className="text-sm font-medium text-tipx-ink/43">
            No coordinated distributions
          </p>
          <p className="mt-2 text-[10px] text-tipx-ink/24">
            The serving contract has not supplied analytical buckets for this population.
          </p>
        </div>
      </div>
    )
  }

  return (
    <div className="grid gap-4 xl:grid-cols-2">
      {distributions.slice(0, 6).map((distribution) => (
        <DistributionField
          key={distribution.id}
          distribution={distribution}
          filters={filters}
          onFilter={onFilter}
          onPopulation={onPopulation}
        />
      ))}
    </div>
  )
}

function DistributionField({
  distribution,
  filters,
  onFilter,
  onPopulation,
}: {
  distribution: AnalyticsDistribution
  filters: AnalyticsFilter[]
  onFilter: (filter: AnalyticsFilter) => void
  onPopulation: (population: PopulationRef) => void
}) {
  const buckets = React.useMemo(
    () =>
      [...distribution.buckets]
        .sort((left, right) => right.count - left.count)
        .slice(0, 16),
    [distribution.buckets]
  )

  const option = React.useMemo<EChartsOption>(
    () => ({
      animationDuration: 360,
      animationEasing: "cubicOut",
      backgroundColor: "transparent",
      grid: {
        left: 8,
        right: 14,
        top: 8,
        bottom: 8,
        containLabel: true,
      },
      tooltip: {
        trigger: "item",
        backgroundColor: "rgba(8,16,22,.97)",
        borderColor: "rgba(255,255,255,.1)",
        textStyle: { color: "rgba(255,255,255,.76)", fontSize: 10 },
      },
      xAxis: {
        type: "value",
        splitLine: { lineStyle: { color: "rgba(255,255,255,.045)" } },
        axisLabel: { color: "rgba(255,255,255,.22)", fontSize: 8 },
      },
      yAxis: {
        type: "category",
        inverse: true,
        data: buckets.map((bucket) => bucket.label),
        axisTick: { show: false },
        axisLine: { show: false },
        axisLabel: {
          color: "rgba(255,255,255,.38)",
          fontSize: 9,
          width: 116,
          overflow: "truncate",
        },
      },
      series: [
        {
          type: "bar",
          barMaxWidth: 14,
          data: buckets.map((bucket) => ({
            value: bucket.count,
            itemStyle: {
              color: bucketIsActive(bucket, filters)
                ? "rgba(255,255,255,.92)"
                : "rgba(139,202,220,.48)",
              borderRadius: [0, 6, 6, 0],
            },
          })),
        },
      ],
    }),
    [buckets, filters]
  )

  const onEvents = React.useMemo(
    () => ({
      click: (params: { dataIndex?: number }) => {
        const bucket =
          typeof params.dataIndex === "number"
            ? buckets[params.dataIndex]
            : undefined
        if (!bucket) return

        if (bucket.population) {
          onPopulation(bucket.population)
          return
        }

        if (bucket.filter) onFilter(bucket.filter)
      },
    }),
    [buckets, onFilter, onPopulation]
  )

  return (
    <section className="overflow-hidden rounded-[24px] border border-tipx-line/8 bg-tipx-surface/70 p-4 backdrop-blur-xl">
      <div className="mb-3 flex items-center justify-between gap-3">
        <div>
          <p className="text-[9px] font-semibold uppercase tracking-[0.18em] text-tipx-ink/28">
            {distribution.label}
          </p>
          <p className="mt-1 text-[9px] text-tipx-ink/18">
            Click a semantic bucket to coordinate the analytical population
          </p>
        </div>
        <span className="font-mono text-[8px] text-tipx-ink/18">
          {distribution.buckets.length} buckets
        </span>
      </div>
      <ReactECharts
        option={option}
        onEvents={onEvents}
        notMerge
        lazyUpdate
        style={{ height: 250, width: "100%" }}
      />
    </section>
  )
}

function bucketIsActive(
  bucket: AnalyticsBucket,
  filters: AnalyticsFilter[]
) {
  if (!bucket.filter) return false

  return filters.some(
    (filter) =>
      filter.dimension === bucket.filter?.dimension &&
      filter.value === bucket.filter.value
  )
}
