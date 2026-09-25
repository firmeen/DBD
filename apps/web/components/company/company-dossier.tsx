"use client"

import {
  ArrowUpRight,
  Building2,
  CalendarDays,
  Landmark,
  MapPin,
  Network,
  ShieldCheck,
  Users,
} from "lucide-react"

import { formatCompanyDate, formatCompanyMetric } from "@/lib/company/format"
import type { CompanyDetailSnapshot } from "@/lib/company/types"
import type { EntityRef, SurfaceId } from "@/lib/intelligence/types"
import { cn } from "@/lib/utils"

export function CompanyDossier({
  detail,
  fallbackEntity,
  onHandoff,
}: {
  detail: CompanyDetailSnapshot
  fallbackEntity: EntityRef
  onHandoff: (surface: SurfaceId) => void
}) {
  const profile = detail.profile

  if (!profile) {
    return (
      <UnavailableCompanyDetail
        entity={fallbackEntity}
        message={detail.message}
        onHandoff={onHandoff}
      />
    )
  }

  const metrics = [
    {
      label: "Registered capital",
      value: formatCompanyMetric(profile.registeredCapital, "currency", true),
    },
    {
      label: "Total assets",
      value: formatCompanyMetric(profile.totalAssets, "currency", true),
    },
    {
      label: "Total equity",
      value: formatCompanyMetric(profile.totalEquity, "currency", true),
    },
    {
      label: "Total income",
      value: formatCompanyMetric(profile.totalIncome, "currency", true),
    },
    {
      label: "Net profit",
      value: formatCompanyMetric(profile.netProfit, "currency", true),
    },
  ]

  return (
    <div className="space-y-5">
      <section className="overflow-hidden rounded-[28px] border border-tipx-line/9 bg-tipx-surface/76 shadow-[0_35px_120px_rgba(0,0,0,.28)] backdrop-blur-2xl">
        <div className="grid gap-8 px-6 pb-6 pt-7 lg:grid-cols-[1.35fr_.65fr] lg:px-8 lg:pb-8">
          <div>
            <div className="flex flex-wrap items-center gap-2">
              <StatusPill value={profile.statusDescription} />
              {profile.typeDescription ? (
                <span className="rounded-full border border-tipx-line/8 bg-tipx-ink/[0.025] px-2.5 py-1 text-[9px] uppercase tracking-[0.14em] text-tipx-ink/34">
                  {profile.typeDescription}
                </span>
              ) : null}
              {profile.latestFiscalYear ? (
                <span className="rounded-full border border-tipx-line/8 bg-tipx-ink/[0.025] px-2.5 py-1 font-mono text-[9px] text-tipx-ink/34">
                  FY {profile.latestFiscalYear}
                </span>
              ) : null}
            </div>

            <p className="mt-5 font-mono text-[10px] uppercase tracking-[0.24em] text-tipx-ink/26">
              Juristic person · {profile.id}
            </p>
            <h1 className="mt-2 max-w-5xl text-balance text-3xl font-semibold leading-[1.08] tracking-[-0.035em] text-tipx-ink sm:text-5xl">
              {profile.nameTh}
            </h1>
            {profile.nameEn ? (
              <p className="mt-2 max-w-4xl text-sm leading-6 text-tipx-ink/36">
                {profile.nameEn}
              </p>
            ) : null}

            <div className="mt-7 flex flex-wrap items-center gap-x-5 gap-y-2 text-[11px] text-tipx-ink/42">
              {profile.businessTypeDescription ? (
                <span className="inline-flex items-center gap-2">
                  <Building2 className="size-3.5 text-tipx-ink/26" />
                  {profile.businessTypeDescription}
                </span>
              ) : null}
              {profile.province ? (
                <span className="inline-flex items-center gap-2">
                  <MapPin className="size-3.5 text-tipx-ink/26" />
                  {[profile.district, profile.province]
                    .filter(Boolean)
                    .join(" · ")}
                </span>
              ) : null}
              {profile.registrationDate ? (
                <span className="inline-flex items-center gap-2">
                  <CalendarDays className="size-3.5 text-tipx-ink/26" />
                  Registered {formatCompanyDate(profile.registrationDate)}
                </span>
              ) : null}
            </div>
          </div>

          <div className="flex flex-col justify-end">
            <p className="text-[9px] font-semibold uppercase tracking-[0.2em] text-tipx-ink/26">
              Represent this entity as
            </p>
            <div className="mt-3 grid grid-cols-5 gap-1.5">
              {([
                ["M", "Map"],
                ["P", "People"],
                ["R", "Relation"],
                ["A", "Analytics"],
                ["O", "Opportunity"],
              ] as const).map(([surface, label]) => (
                <button
                  key={surface}
                  type="button"
                  onClick={() => onHandoff(surface)}
                  className="group rounded-xl border border-tipx-line/8 bg-tipx-ink/[0.025] px-2 py-2.5 text-center transition hover:border-tipx-line/18 hover:bg-tipx-ink/[0.065]"
                  aria-label={`Open ${label}`}
                >
                  <span className="block font-mono text-[11px] font-semibold text-tipx-ink/68 group-hover:text-tipx-ink">
                    {surface}
                  </span>
                  <span className="mt-0.5 hidden text-[8px] text-tipx-ink/24 xl:block">
                    {label}
                  </span>
                </button>
              ))}
            </div>
          </div>
        </div>

        <div className="grid border-t border-tipx-line/8 md:grid-cols-5">
          {metrics.map((metric) => (
            <div
              key={metric.label}
              className="border-b border-tipx-line/7 px-5 py-4 md:border-b-0 md:border-r md:last:border-r-0"
            >
              <p className="font-mono text-base font-medium tracking-[-0.03em] text-tipx-ink/78">
                {metric.value}
              </p>
              <p className="mt-1 text-[8px] uppercase tracking-[0.15em] text-tipx-ink/24">
                {metric.label}
              </p>
            </div>
          ))}
        </div>
      </section>

      <section className="grid gap-5 lg:grid-cols-[1.25fr_.75fr]">
        <div className="rounded-[24px] border border-tipx-line/8 bg-tipx-ink/[0.018] p-6">
          <SectionEyebrow icon={Landmark} label="Business context" />
          <dl className="mt-5 grid gap-5 sm:grid-cols-2">
            <DossierField
              label="Business type"
              value={profile.businessTypeDescription}
            />
            <DossierField
              label="Business size code"
              value={profile.businessSizeCode}
            />
            <DossierField
              label="Registered capital"
              value={formatCompanyMetric(
                profile.registeredCapital,
                "currency"
              )}
            />
            <DossierField
              label="Paid capital"
              value={formatCompanyMetric(profile.paidCapital, "currency")}
            />
          </dl>

          <div className="mt-6 border-t border-tipx-line/7 pt-5">
            <p className="text-[9px] font-semibold uppercase tracking-[0.18em] text-tipx-ink/28">
              Business objective
            </p>
            <p className="mt-2 max-w-4xl text-[12px] leading-6 text-tipx-ink/52">
              {profile.setupObjective || "No serving value available."}
            </p>
          </div>

          <div className="mt-6 border-t border-tipx-line/7 pt-5">
            <p className="text-[9px] font-semibold uppercase tracking-[0.18em] text-tipx-ink/28">
              Registered address
            </p>
            <p className="mt-2 max-w-4xl text-[12px] leading-6 text-tipx-ink/52">
              {profile.address ||
                [
                  profile.subdistrict,
                  profile.district,
                  profile.province,
                  profile.postalCode,
                ]
                  .filter(Boolean)
                  .join(" · ") ||
                "No serving value available."}
            </p>
          </div>
        </div>

        <div className="rounded-[24px] border border-tipx-line/8 bg-tipx-ink/[0.018] p-6">
          <SectionEyebrow icon={ShieldCheck} label="Connected intelligence" />

          <div className="mt-5 space-y-3">
            <ContextRow
              icon={ShieldCheck}
              label="TIP status"
              value={detail.tipContext?.status ?? "Unavailable"}
              active={detail.capabilities.tipContext}
            />
            <ContextRow
              icon={Users}
              label="Board members"
              value={
                detail.boardSummary?.memberCount !== null &&
                detail.boardSummary?.memberCount !== undefined
                  ? detail.boardSummary.memberCount.toLocaleString()
                  : "Unavailable"
              }
              active={detail.capabilities.boardSummary}
            />
            <ContextRow
              icon={Network}
              label="Corporate history"
              value={
                detail.capabilities.corporateHistory
                  ? `${detail.history.length} events`
                  : "Unavailable"
              }
              active={detail.capabilities.corporateHistory}
            />
          </div>

          <div className="mt-6 grid grid-cols-2 gap-2 border-t border-tipx-line/7 pt-5">
            <button
              type="button"
              onClick={() => onHandoff("P")}
              className="inline-flex items-center justify-between rounded-xl border border-tipx-line/8 bg-tipx-ink/[0.025] px-3 py-3 text-[10px] text-tipx-ink/48 transition hover:bg-tipx-ink/[0.06] hover:text-tipx-ink"
            >
              People
              <ArrowUpRight className="size-3" />
            </button>
            <button
              type="button"
              onClick={() => onHandoff("R")}
              className="inline-flex items-center justify-between rounded-xl border border-tipx-line/8 bg-tipx-ink/[0.025] px-3 py-3 text-[10px] text-tipx-ink/48 transition hover:bg-tipx-ink/[0.06] hover:text-tipx-ink"
            >
              Relationship
              <ArrowUpRight className="size-3" />
            </button>
          </div>
        </div>
      </section>
    </div>
  )
}

function StatusPill({ value }: { value?: string | null }) {
  return (
    <span className="rounded-full border border-tipx-line/9 bg-tipx-ink/[0.025] px-2.5 py-1 text-[9px] font-semibold uppercase tracking-[0.14em] text-tipx-ink/46">
      {value || "Status unavailable"}
    </span>
  )
}

function SectionEyebrow({
  icon: Icon,
  label,
}: {
  icon: typeof Landmark
  label: string
}) {
  return (
    <div className="flex items-center gap-2 text-[9px] font-semibold uppercase tracking-[0.2em] text-tipx-ink/27">
      <Icon className="size-3.5" />
      {label}
    </div>
  )
}

function DossierField({
  label,
  value,
}: {
  label: string
  value?: string | null
}) {
  return (
    <div>
      <dt className="text-[9px] uppercase tracking-[0.16em] text-tipx-ink/25">
        {label}
      </dt>
      <dd className="mt-1.5 text-[12px] leading-5 text-tipx-ink/58">
        {value || "—"}
      </dd>
    </div>
  )
}

function ContextRow({
  icon: Icon,
  label,
  value,
  active,
}: {
  icon: typeof ShieldCheck
  label: string
  value: string
  active: boolean
}) {
  return (
    <div className="flex items-center gap-3 rounded-xl border border-tipx-line/7 bg-tipx-ink/[0.025] px-3 py-3">
      <span className="grid size-8 shrink-0 place-items-center rounded-lg border border-tipx-line/7 bg-tipx-ink/[0.025]">
        <Icon className="size-3.5 text-tipx-ink/38" />
      </span>
      <span className="min-w-0 flex-1">
        <span className="block text-[9px] uppercase tracking-[0.13em] text-tipx-ink/25">
          {label}
        </span>
        <span className="mt-0.5 block truncate text-[11px] text-tipx-ink/58">
          {value}
        </span>
      </span>
      <span
        className={cn(
          "size-1.5 rounded-full",
          active ? "bg-tipx-success/70" : "bg-tipx-ink/16"
        )}
      />
    </div>
  )
}

function UnavailableCompanyDetail({
  entity,
  message,
  onHandoff,
}: {
  entity: EntityRef
  message?: string
  onHandoff: (surface: SurfaceId) => void
}) {
  return (
    <section className="rounded-[28px] border border-tipx-line/9 bg-tipx-surface/76 px-6 py-8 backdrop-blur-2xl sm:px-8">
      <p className="font-mono text-[10px] uppercase tracking-[0.24em] text-tipx-ink/26">
        Focused company · {entity.id}
      </p>
      <h1 className="mt-3 text-3xl font-semibold tracking-[-0.035em] text-tipx-ink sm:text-5xl">
        {entity.label}
      </h1>
      {entity.secondary ? (
        <p className="mt-2 text-sm text-tipx-ink/36">{entity.secondary}</p>
      ) : null}

      <div className="mt-8 max-w-2xl rounded-2xl border border-tipx-warning/18 bg-tipx-warning/7 p-5">
        <p className="text-[10px] font-semibold uppercase tracking-[0.18em] text-tipx-warning">
          Company serving detail unavailable
        </p>
        <p className="mt-2 text-[12px] leading-6 text-tipx-ink/38">
          {message ||
            "The entity remains focused, but TIPX will not fabricate profile or financial values while the Center Database company contract is unavailable."}
        </p>
      </div>

      <div className="mt-6 flex gap-2">
        {(["M", "R", "A"] as const).map((surface) => (
          <button
            key={surface}
            type="button"
            onClick={() => onHandoff(surface)}
            className="rounded-xl border border-tipx-line/8 bg-tipx-ink/[0.025] px-3 py-2 text-[10px] text-tipx-ink/44 hover:bg-tipx-ink/[0.06] hover:text-tipx-ink"
          >
            {surface}
          </button>
        ))}
      </div>
    </section>
  )
}
