"use client"

import * as React from "react"
import { useVirtualizer } from "@tanstack/react-virtual"
import {
  ArrowUpRight,
  Building2,
  GitBranch,
  Network,
  UserRound,
  UsersRound,
} from "lucide-react"

import type {
  PersonCompanyRole,
  PersonDetailSnapshot,
  SharedDirectorConnection,
} from "@/lib/relationship/types"
import type { EntityRef, SurfaceId } from "@/lib/intelligence/types"
import { cn } from "@/lib/utils"

export function PersonDossier({
  detail,
  fallbackEntity,
  onFocus,
  onHandoff,
  onOpenMatrix,
}: {
  detail: PersonDetailSnapshot
  fallbackEntity: EntityRef
  onFocus: (entity: EntityRef) => void
  onHandoff: (surface: SurfaceId) => void
  onOpenMatrix: () => void
}) {
  const profile = detail.profile
  const personName = profile?.name ?? fallbackEntity.label

  return (
    <div className="grid gap-4 xl:grid-cols-[minmax(0,1.15fr)_minmax(360px,.85fr)]">
      <section className="rounded-[28px] border border-tipx-line/8 bg-tipx-surface/76 p-6 backdrop-blur-2xl sm:p-7">
        <div className="flex flex-col gap-6 lg:flex-row lg:items-start lg:justify-between">
          <div className="min-w-0">
            <p className="font-mono text-[9px] uppercase tracking-[0.22em] text-tipx-ink/24">
              Canonical person
            </p>
            <div className="mt-3 flex items-start gap-4">
              <span className="grid size-12 shrink-0 place-items-center rounded-2xl border border-tipx-line/9 bg-tipx-ink/[0.035]">
                <UserRound className="size-5 text-tipx-ink/42" />
              </span>
              <div className="min-w-0">
                <h1 className="text-balance text-3xl font-semibold tracking-[-0.04em] text-tipx-ink sm:text-4xl">
                  {personName}
                </h1>
                <p className="mt-2 max-w-2xl text-[11px] leading-5 text-tipx-ink/31">
                  {profile?.secondary ||
                    fallbackEntity.secondary ||
                    "Person identity resolved through the relationship serving model."}
                </p>
              </div>
            </div>
          </div>

          <div className="flex shrink-0 gap-1.5">
            <Action code="R" label="Relationship" onClick={() => onHandoff("R")} />
            <Action code="M" label="Map" onClick={() => onHandoff("M")} />
            <button
              type="button"
              onClick={onOpenMatrix}
              className="rounded-xl border border-tipx-line/8 bg-tipx-ink/[0.025] px-3 py-2 text-[9px] font-medium text-tipx-ink/44 transition hover:bg-tipx-ink/[0.065] hover:text-tipx-ink"
            >
              Matrix
            </button>
          </div>
        </div>

        <div className="mt-7 grid grid-cols-3 border-y border-tipx-line/7">
          <Metric label="Companies" value={profile?.companyCount} />
          <Metric label="Active" value={profile?.activeCompanyCount ?? null} />
          <Metric label="TIP context" value={profile?.tipCompanyCount ?? null} />
        </div>

        {detail.availability === "partial" ? (
          <p className="mt-4 text-[9px] uppercase tracking-[0.14em] text-amber-100/48">
            Partial relationship serving data
          </p>
        ) : null}

        <div className="mt-7">
          <SectionHeading
            icon={Building2}
            title="Company field"
            detail="Current loaded company-role population"
          />
          <CompanyRoleRail companies={detail.companies} onFocus={onFocus} />
        </div>
      </section>

      <section className="rounded-[28px] border border-tipx-line/8 bg-tipx-surface/68 p-6 backdrop-blur-2xl sm:p-7">
        <SectionHeading
          icon={UsersRound}
          title="Shared-director field"
          detail="People connected through common company memberships"
        />

        {detail.sharedDirectors.length > 0 ? (
          <div className="mt-5 space-y-2">
            {detail.sharedDirectors.slice(0, 48).map((connection) => (
              <SharedDirectorRow
                key={connection.person.id}
                connection={connection}
                onFocus={onFocus}
              />
            ))}
          </div>
        ) : (
          <EmptyRelationship
            icon={GitBranch}
            title="No shared-director context"
            detail={
              detail.message ||
              "The serving model returned no shared-director connections for this person."
            }
          />
        )}

        <div className="mt-6 rounded-2xl border border-tipx-line/7 bg-tipx-ink/[0.02] p-4">
          <div className="flex items-center gap-2 text-[9px] font-semibold uppercase tracking-[0.16em] text-tipx-ink/26">
            <Network className="size-3.5" />
            Representation handoff
          </div>
          <p className="mt-2 text-[10px] leading-5 text-tipx-ink/30">
            The person dossier keeps identity compact. Multi-hop connectivity,
            common-director expansion and shortest-path investigation belong to
            Relationship rather than duplicating a graph here.
          </p>
        </div>
      </section>
    </div>
  )
}

function CompanyRoleRail({
  companies,
  onFocus,
}: {
  companies: PersonCompanyRole[]
  onFocus: (entity: EntityRef) => void
}) {
  const scrollRef = React.useRef<HTMLDivElement>(null)
  const virtualizer = useVirtualizer({
    count: companies.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => 70,
    overscan: 8,
  })

  if (companies.length === 0) {
    return (
      <EmptyRelationship
        icon={Building2}
        title="No company roles"
        detail="No company-role rows are available for this person under the current Lens."
      />
    )
  }

  return (
    <div
      ref={scrollRef}
      className="mt-4 h-[min(42vh,430px)] overflow-auto rounded-2xl border border-tipx-line/7 bg-tipx-ink/[0.025]"
    >
      <div
        className="relative w-full"
        style={{ height: virtualizer.getTotalSize() }}
      >
        {virtualizer.getVirtualItems().map((virtualRow) => {
          const role = companies[virtualRow.index]

          return (
            <button
              key={role.company.id + ":" + virtualRow.index}
              ref={virtualizer.measureElement}
              data-index={virtualRow.index}
              type="button"
              onClick={() => onFocus(role.company)}
              className="absolute left-0 top-0 flex w-full items-center gap-3 border-b border-tipx-line/6 px-4 py-3 text-left transition hover:bg-tipx-ink/[0.045]"
              style={{
                transform: "translateY(" + virtualRow.start + "px)",
              }}
            >
              <span className="grid size-9 shrink-0 place-items-center rounded-xl border border-tipx-line/8 bg-tipx-ink/[0.025] text-[11px] font-semibold text-tipx-ink/44">
                {role.company.label.trim().charAt(0).toUpperCase() || "?"}
              </span>
              <span className="min-w-0 flex-1">
                <span className="block truncate text-[11px] font-medium text-tipx-ink/64">
                  {role.company.label}
                </span>
                <span className="mt-1 block truncate text-[9px] text-tipx-ink/27">
                  {[role.role, role.companyStatus, role.latestFiscalYear ? "FY " + role.latestFiscalYear : null]
                    .filter(Boolean)
                    .join(" · ") || "Board relationship"}
                </span>
              </span>
              <span
                className={cn(
                  "rounded-full border px-2 py-1 text-[8px] uppercase tracking-[0.1em]",
                  role.tipStatus === "active"
                    ? "border-cyan-200/15 text-cyan-100/55"
                    : role.tipStatus === "expired"
                      ? "border-rose-200/12 text-rose-100/45"
                      : "border-tipx-line/8 text-tipx-ink/25"
                )}
              >
                {role.tipStatus ?? "TIP —"}
              </span>
              <ArrowUpRight className="size-3.5 shrink-0 text-tipx-ink/18" />
            </button>
          )
        })}
      </div>
    </div>
  )
}

function SharedDirectorRow({
  connection,
  onFocus,
}: {
  connection: SharedDirectorConnection
  onFocus: (entity: EntityRef) => void
}) {
  return (
    <button
      type="button"
      onClick={() => onFocus(connection.person)}
      className="flex w-full items-center gap-3 rounded-2xl border border-tipx-line/7 bg-tipx-ink/[0.018] px-3 py-3 text-left transition hover:border-tipx-line/12 hover:bg-tipx-ink/[0.05]"
    >
      <span className="grid size-9 shrink-0 place-items-center rounded-xl border border-tipx-line/8 bg-tipx-ink/[0.025]">
        <UserRound className="size-3.5 text-tipx-ink/35" />
      </span>
      <span className="min-w-0 flex-1">
        <span className="block truncate text-[11px] font-medium text-tipx-ink/61">
          {connection.person.label}
        </span>
        <span className="mt-0.5 block truncate text-[9px] text-tipx-ink/25">
          {connection.companies
            .slice(0, 3)
            .map((company) => company.label)
            .join(" · ") || "Shared board context"}
        </span>
      </span>
      <span className="font-mono text-[9px] text-tipx-ink/31">
        ×{connection.sharedCompanyCount}
      </span>
    </button>
  )
}

function Metric({
  label,
  value,
}: {
  label: string
  value: number | null | undefined
}) {
  return (
    <div className="border-r border-tipx-line/7 px-3 py-4 last:border-r-0">
      <p className="font-mono text-lg text-tipx-ink/72">
        {value == null ? "—" : value.toLocaleString()}
      </p>
      <p className="mt-1 text-[8px] uppercase tracking-[0.14em] text-tipx-ink/23">
        {label}
      </p>
    </div>
  )
}

function SectionHeading({
  icon: Icon,
  title,
  detail,
}: {
  icon: typeof Building2
  title: string
  detail: string
}) {
  return (
    <div className="flex items-center gap-3">
      <span className="grid size-9 place-items-center rounded-xl border border-tipx-line/8 bg-tipx-ink/[0.025]">
        <Icon className="size-3.5 text-tipx-ink/35" />
      </span>
      <div>
        <p className="text-[11px] font-medium text-tipx-ink/60">{title}</p>
        <p className="mt-0.5 text-[9px] text-tipx-ink/24">{detail}</p>
      </div>
    </div>
  )
}

function EmptyRelationship({
  icon: Icon,
  title,
  detail,
}: {
  icon: typeof Building2
  title: string
  detail: string
}) {
  return (
    <div className="mt-5 rounded-2xl border border-dashed border-tipx-line/8 px-5 py-8 text-center">
      <Icon className="mx-auto size-5 text-tipx-ink/17" />
      <p className="mt-3 text-[11px] font-medium text-tipx-ink/45">{title}</p>
      <p className="mx-auto mt-1 max-w-sm text-[9px] leading-4 text-tipx-ink/24">
        {detail}
      </p>
    </div>
  )
}

function Action({
  code,
  label,
  onClick,
}: {
  code: string
  label: string
  onClick: () => void
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="group inline-flex items-center gap-2 rounded-xl border border-tipx-line/8 bg-tipx-ink/[0.025] px-3 py-2 text-[9px] text-tipx-ink/40 transition hover:bg-tipx-ink/[0.065] hover:text-tipx-ink"
      aria-label={label}
    >
      <span className="font-mono font-semibold">{code}</span>
      <span className="hidden xl:inline">{label}</span>
    </button>
  )
}
