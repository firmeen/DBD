"use client"

import {
  ArrowRight,
  Building2,
  CalendarClock,
  GitMerge,
  Landmark,
  RefreshCw,
} from "lucide-react"

import { formatCompanyDate, formatCompanyMetric } from "@/lib/company/format"
import type { CompanyHistoryEvent } from "@/lib/company/types"
import type { EntityRef, SurfaceId } from "@/lib/intelligence/types"

export function CompanyHistory({
  events,
  onFocusCompany,
  onHandoff,
}: {
  events: CompanyHistoryEvent[]
  onFocusCompany: (company: EntityRef) => void
  onHandoff: (surface: SurfaceId) => void
}) {
  if (events.length === 0) {
    return (
      <HistoryEmpty message="Corporate-history serving capability is unavailable for this company." />
    )
  }

  const chronological = [...events].sort((left, right) => {
    if (!left.date && !right.date) return 0
    if (!left.date) return 1
    if (!right.date) return -1
    return right.date.localeCompare(left.date)
  })

  return (
    <section className="rounded-[26px] border border-white/8 bg-[#0a1015]/72 p-6 backdrop-blur-2xl sm:p-7">
      <div className="max-w-3xl">
        <p className="flex items-center gap-2 text-[9px] font-semibold uppercase tracking-[0.2em] text-white/27">
          <CalendarClock className="size-3.5" />
          Corporate event rail
        </p>
        <h2 className="mt-2 text-2xl font-semibold tracking-[-0.035em] text-white">
          Changes are events, not another table
        </h2>
        <p className="mt-2 text-[11px] leading-5 text-white/31">
          Name, capital, status, submission and transformation context stay on
          one chronology. Directed genealogy itself hands off to Relationship.
        </p>
      </div>

      <div className="relative mt-8 pl-8 before:absolute before:bottom-2 before:left-[11px] before:top-2 before:w-px before:bg-white/8">
        {chronological.map((event) => (
          <HistoryEventRow
            key={event.id}
            event={event}
            onFocusCompany={onFocusCompany}
            onHandoff={onHandoff}
          />
        ))}
      </div>
    </section>
  )
}

function HistoryEventRow({
  event,
  onFocusCompany,
  onHandoff,
}: {
  event: CompanyHistoryEvent
  onFocusCompany: (company: EntityRef) => void
  onHandoff: (surface: SurfaceId) => void
}) {
  const Icon = historyIcon(event.kind)

  return (
    <article className="relative pb-7 last:pb-0">
      <span className="absolute -left-8 top-0 grid size-[23px] place-items-center rounded-full border border-white/10 bg-[#0c1319] shadow-[0_0_0_5px_#0a1015]">
        <Icon className="size-3 text-white/42" />
      </span>

      <div className="grid gap-3 lg:grid-cols-[140px_1fr_auto] lg:items-start">
        <time className="font-mono text-[9px] uppercase tracking-[0.12em] text-white/25">
          {formatCompanyDate(event.date)}
        </time>
        <div>
          <p className="text-[12px] font-medium text-white/61">
            {event.title}
          </p>
          {event.detail ? (
            <p className="mt-1 max-w-3xl text-[11px] leading-5 text-white/31">
              {event.detail}
            </p>
          ) : null}

          {event.previousAmount !== null ||
          event.newAmount !== null ? (
            <div className="mt-3 inline-flex items-center gap-2 rounded-lg border border-white/7 bg-white/[0.025] px-2.5 py-1.5 font-mono text-[9px] text-white/36">
              {formatCompanyMetric(
                event.previousAmount,
                "currency",
                true
              )}
              <ArrowRight className="size-3 text-white/18" />
              <span className="text-white/62">
                {formatCompanyMetric(event.newAmount, "currency", true)}
              </span>
            </div>
          ) : null}
        </div>

        {event.relatedCompany ? (
          <div className="flex gap-1.5">
            <button
              type="button"
              onClick={() => onFocusCompany(event.relatedCompany!)}
              className="rounded-lg border border-white/8 bg-white/[0.025] px-2.5 py-2 text-[9px] text-white/40 hover:bg-white/[0.06] hover:text-white"
            >
              Focus company
            </button>
            {(event.kind === "transform" || event.kind === "merger") ? (
              <button
                type="button"
                onClick={() => onHandoff("R")}
                className="rounded-lg border border-white/8 bg-white/[0.025] px-2.5 py-2 font-mono text-[9px] text-white/40 hover:bg-white/[0.06] hover:text-white"
              >
                R
              </button>
            ) : null}
          </div>
        ) : null}
      </div>
    </article>
  )
}

function historyIcon(kind: CompanyHistoryEvent["kind"]) {
  switch (kind) {
    case "capital":
      return Landmark
    case "transform":
    case "merger":
      return GitMerge
    case "submission":
      return RefreshCw
    default:
      return Building2
  }
}

function HistoryEmpty({ message }: { message: string }) {
  return (
    <div className="grid min-h-[460px] place-items-center rounded-[26px] border border-white/8 bg-white/[0.018] px-6 text-center">
      <div>
        <CalendarClock className="mx-auto size-6 text-white/18" />
        <p className="mt-3 text-sm font-medium text-white/52">
          Corporate history unavailable
        </p>
        <p className="mx-auto mt-2 max-w-md text-[11px] leading-5 text-white/28">
          {message}
        </p>
      </div>
    </div>
  )
}
