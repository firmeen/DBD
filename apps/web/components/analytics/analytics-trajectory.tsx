"use client"

import * as React from "react"
import type { EChartsOption } from "echarts"
import ReactECharts from "echarts-for-react"
import { Activity } from "lucide-react"

import { useTipxTheme } from "@/hooks/use-tipx-theme"
import { formatAnalyticsMetric } from "@/lib/analytics/format"
import type {
  AnalyticsMetricFormat,
  AnalyticsSeries,
} from "@/lib/analytics/types"

export function AnalyticsTrajectory({
  series,
}: {
  series: AnalyticsSeries[]
}) {
  const groups = React.useMemo(() => {
    const grouped = new Map<AnalyticsMetricFormat, AnalyticsSeries[]>()
    for (const item of series) {
      const current = grouped.get(item.format) ?? []
      current.push(item)
      grouped.set(item.format, current)
    }
    return [...grouped.entries()]
  }, [series])

  if (groups.length === 0) return null

  return (
    <section className="rounded-[26px] border border-tipx-line/8 bg-tipx-surface/70 p-4 backdrop-blur-xl sm:p-5">
      <div className="mb-4">
        <p className="flex items-center gap-2 text-[9px] font-semibold uppercase tracking-[0.19em] text-tipx-ink/28">
          <Activity className="size-3.5" />
          Population trajectory
        </p>
        <p className="mt-1 text-[9px] text-tipx-ink/20">
          Metric families stay on separate units; no mixed currency/percentage axis.
        </p>
      </div>
      <div className="grid gap-4 xl:grid-cols-2">
        {groups.map(([format, metrics]) => (
          <TrajectoryGroup
            key={format}
            format={format}
            series={metrics}
          />
        ))}
      </div>
    </section>
  )
}

function TrajectoryGroup({
  format,
  series,
}: {
  format: AnalyticsMetricFormat
  series: AnalyticsSeries[]
}) {
  const { palette } = useTipxTheme()

  const option = React.useMemo<EChartsOption>(() => {
    const periods = Array.from(
      new Set(series.flatMap((item) => item.points.map((point) => point.period)))
    ).sort()

    return {
      color: [
        palette.chart.primary,
        palette.chart.secondary,
        palette.chart.accent,
      ],
      animationDuration: 360,
      grid: { left: 8, right: 14, top: 42, bottom: 16, containLabel: true },
      legend: {
        top: 0,
        left: 0,
        type: "scroll",
        textStyle: { color: palette.chart.label, fontSize: 8 },
      },
      tooltip: {
        trigger: "axis",
        backgroundColor: palette.chart.tooltipBackground,
        borderColor: palette.chart.tooltipBorder,
        textStyle: { color: palette.chart.tooltipText, fontSize: 10 },
      },
      xAxis: {
        type: "category",
        data: periods,
        boundaryGap: false,
        axisTick: { show: false },
        axisLine: { lineStyle: { color: palette.chart.axis } },
        axisLabel: { color: palette.chart.muted, fontSize: 8 },
      },
      yAxis: {
        type: "value",
        splitLine: { lineStyle: { color: palette.chart.grid } },
        axisLabel: {
          color: palette.chart.muted,
          fontSize: 8,
          formatter: (value: number | string) =>
            formatAnalyticsMetric(Number(value), format, true),
        },
      },
      series: series.map((item, index) => ({
        name: item.label,
        type: "line",
        smooth: 0.2,
        showSymbol: false,
        lineStyle: {
          width: index === 0 ? 2.2 : 1.4,
          opacity: index === 0 ? 0.9 : 0.55,
        },
        data: periods.map(
          (period) =>
            item.points.find((point) => point.period === period)?.value ?? null
        ),
      })),
    }
  }, [format, palette, series])

  return (
    <div className="rounded-2xl border border-tipx-line/7 bg-tipx-ink/[0.025] p-3">
      <p className="mb-2 font-mono text-[8px] uppercase tracking-[0.14em] text-tipx-ink/18">
        {format}
      </p>
      <ReactECharts
        option={option}
        notMerge
        lazyUpdate
        style={{ height: 260, width: "100%" }}
      />
    </div>
  )
}
