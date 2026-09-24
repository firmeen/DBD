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
    <section className="rounded-[26px] border border-tipx-line/8 bg-tipx-surface/72 p-6 backdrop-blur-2xl sm:p-7">
      <div className="max-w-3xl">
        <p className="flex items-center gap-2 text-[9px] font-semibold uppercase tracking-[0.2em] text-tipx-ink/27">
          <CalendarClock className="size-3.5" />
          Corporate event rail
        </p>
        <h2 className="mt-2 text-2xl font-semibold tracking-[-0.035em] text-tipx-ink">
          Changes are events, not another table
        </h2>
        <p className="mt-2 text-[11px] leading-5 text-tipx-ink/31">
          Name, capital, status, submission and transformation context stay on
          one chronology. Directed genealogy itself hands off to Relationship.
        </p>
      </div>

      <div className="relative mt-8 pl-8 before:absolute before:bottom-2 before:left-[11px] before:top-2 before:w-px before:bg-tipx-ink/8">
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
  return (
    <article className="relative pb-7 last:pb-0">
      <span className="absolute -left-8 top-0 grid size-[23px] place-items-center rounded-full border border-tipx-line/10 bg-tipx-elevated shadow-[0_0_0_5px_var(--tipx-surface)]">
        <HistoryKindIcon kind={event.kind} />
      </span>

      <div className="grid gap-3 lg:grid-cols-[140px_1fr_auto] lg:items-start">
        <time className="font-mono text-[9px] uppercase tracking-[0.12em] text-tipx-ink/25">
          {formatCompanyDate(event.date)}
        </time>
        <div>
          <p className="text-[12px] font-medium text-tipx-ink/61">
            {event.title}
          </p>
          {event.detail ? (
            <p className="mt-1 max-w-3xl text-[11px] leading-5 text-tipx-ink/31">
              {event.detail}
            </p>
          ) : null}

          {event.previousAmount != null ||
          event.newAmount != null ? (
            <div className="mt-3 inline-flex items-center gap-2 rounded-lg border border-tipx-line/7 bg-tipx-ink/[0.025] px-2.5 py-1.5 font-mono text-[9px] text-tipx-ink/36">
              {formatCompanyMetric(
                event.previousAmount,
                "currency",
                true
              )}
              <ArrowRight className="size-3 text-tipx-ink/18" />
              <span className="text-tipx-ink/62">
                {formatCompanyMetric(event.newAmount, "currency", true)}
              </span>
            </div>
          ) : null}
        </div>

        {event.relatedCompany ||
        event.kind === "transform" ||
        event.kind === "merger" ? (
          <div className="flex gap-1.5">
            {event.relatedCompany ? (
              <button
                type="button"
                onClick={() => onFocusCompany(event.relatedCompany!)}
                className="rounded-lg border border-tipx-line/8 bg-tipx-ink/[0.025] px-2.5 py-2 text-[9px] text-tipx-ink/40 hover:bg-tipx-ink/[0.06] hover:text-tipx-ink"
              >
                Focus company
              </button>
            ) : null}
            {event.kind === "transform" || event.kind === "merger" ? (
              <button
                type="button"
                onClick={() => onHandoff("R")}
                className="rounded-lg border border-tipx-line/8 bg-tipx-ink/[0.025] px-2.5 py-2 font-mono text-[9px] text-tipx-ink/40 hover:bg-tipx-ink/[0.06] hover:text-tipx-ink"
                aria-label="Open relationship intelligence for this corporate event"
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

function HistoryKindIcon({
  kind,
}: {
  kind: CompanyHistoryEvent["kind"]
}) {
  const className = "size-3 text-tipx-ink/42"

  switch (kind) {
    case "capital":
      return <Landmark className={className} />
    case "transform":
    case "merger":
      return <GitMerge className={className} />
    case "submission":
      return <RefreshCw className={className} />
    default:
      return <Building2 className={className} />
  }
}

function HistoryEmpty({ message }: { message: string }) {
  return (
    <div className="grid min-h-[460px] place-items-center rounded-[26px] border border-tipx-line/8 bg-tipx-ink/[0.018] px-6 text-center">
      <div>
        <CalendarClock className="mx-auto size-6 text-tipx-ink/18" />
        <p className="mt-3 text-sm font-medium text-tipx-ink/52">
          Corporate history unavailable
        </p>
        <p className="mx-auto mt-2 max-w-md text-[11px] leading-5 text-tipx-ink/28">
          {message}
        </p>
      </div>
    </div>
  )
}
