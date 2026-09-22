"use client"

import * as React from "react"
import {
  Activity,
  Building2,
  ChevronDown,
  ChevronUp,
  CircleDot,
  Layers3,
  LocateFixed,
  Route,
} from "lucide-react"

import { spatialEndpointConfigured } from "@/lib/spatial/adapter"
import { spatialLodLabel } from "@/lib/spatial/lod"
import type {
  SpatialBranch,
  SpatialCapabilities,
  SpatialLayerId,
  SpatialLod,
  SpatialTotals,
} from "@/lib/spatial/types"
import { cn } from "@/lib/utils"

const LAYER_DEFINITIONS: Array<{
  id: SpatialLayerId
  label: string
  capability?: keyof SpatialCapabilities
}> = [
  { id: "administrative", label: "Administrative", capability: "administrativeGeometry" },
  { id: "density", label: "Density", capability: "h3Aggregates" },
  { id: "opportunity", label: "Opportunity", capability: "opportunityOverlay" },
  { id: "companies", label: "Companies", capability: "preciseCompanyGeometry" },
  { id: "branches", label: "Branches", capability: "branches" },
  { id: "catchment", label: "Catchment", capability: "branches" },
  { id: "selected", label: "Selected" },
]

export function SpatialControlDeck({
  lod,
  zoom,
  availability,
  message,
  capabilities,
  totals,
  layers,
  branches,
  branchId,
  radiusKm,
  isFetching,
  onToggleLayer,
  onBranchChange,
  onRadiusChange,
  onHandoff,
}: {
  lod: SpatialLod
  zoom: number
  availability: "ready" | "partial" | "unavailable"
  message?: string
  capabilities: SpatialCapabilities
  totals: SpatialTotals
  layers: SpatialLayerId[]
  branches: SpatialBranch[]
  branchId: string | null
  radiusKm: number
  isFetching: boolean
  onToggleLayer: (layer: SpatialLayerId) => void
  onBranchChange: (branchId: string | null) => void
  onRadiusChange: (radiusKm: number) => void
  onHandoff: (surface: "C" | "R" | "A" | "O") => void
}) {
  const [open, setOpen] = React.useState(false)
  const configured = spatialEndpointConfigured()

  return (
    <aside className="pointer-events-auto fixed left-5 top-5 z-[45] w-[min(360px,calc(100vw-40px))] sm:left-7 sm:top-7">
      <div className="overflow-hidden rounded-2xl border border-white/10 bg-[#081016]/88 shadow-[0_24px_80px_rgba(0,0,0,.36)] backdrop-blur-2xl">
        <button
          type="button"
          onClick={() => setOpen((value) => !value)}
          className="flex w-full items-center gap-3 px-4 py-3.5 text-left"
          aria-expanded={open}
        >
          <span className="grid size-9 shrink-0 place-items-center rounded-xl border border-white/10 bg-white text-sm font-bold text-black">
            M
          </span>
          <span className="min-w-0 flex-1">
            <span className="flex items-center gap-2 text-xs font-semibold text-white">
              Spatial Intelligence
              {isFetching ? (
                <span className="size-1.5 animate-pulse rounded-full bg-cyan-300/80" />
              ) : null}
            </span>
            <span className="mt-0.5 block truncate text-[10px] text-white/38">
              {spatialLodLabel(lod)} · z{zoom.toFixed(1)}
            </span>
          </span>
          {open ? (
            <ChevronUp className="size-4 text-white/35" />
          ) : (
            <ChevronDown className="size-4 text-white/35" />
          )}
        </button>

        <div className="grid grid-cols-4 border-t border-white/8 bg-black/20">
          <Metric label="Companies" value={formatMetric(totals.companies)} />
          <Metric label="Active" value={formatMetric(totals.active)} />
          <Metric label="Expired" value={formatMetric(totals.expired)} />
          <Metric label="Prospect" value={formatMetric(totals.prospect)} />
        </div>

        {open ? (
          <div className="border-t border-white/8 p-4">
            <div className="flex items-start gap-2.5 rounded-xl border border-white/8 bg-white/[0.025] p-3">
              <CircleDot
                className={cn(
                  "mt-0.5 size-3.5 shrink-0",
                  availability === "ready"
                    ? "text-emerald-300"
                    : availability === "partial"
                      ? "text-amber-300"
                      : "text-white/32"
                )}
              />
              <div>
                <p className="text-[10px] font-semibold uppercase tracking-[0.16em] text-white/50">
                  Data boundary
                </p>
                <p className="mt-1 text-[10px] leading-4 text-white/36">
                  {configured
                    ? message || "Spatial serving contract connected."
                    : "No Center Database spatial endpoint configured. Basemap and workspace remain live; analytical geometry is never fabricated."}
                </p>
              </div>
            </div>

            <SectionLabel icon={Layers3} label="Analytical layers" />
            <div className="grid grid-cols-2 gap-1.5">
              {LAYER_DEFINITIONS.map((item) => {
                const supported =
                  !item.capability || capabilities[item.capability]
                const active = layers.includes(item.id)

                return (
                  <button
                    key={item.id}
                    type="button"
                    disabled={!supported && item.id !== "selected"}
                    onClick={() => onToggleLayer(item.id)}
                    className={cn(
                      "rounded-xl border px-3 py-2 text-left text-[10px] transition",
                      active
                        ? "border-white/20 bg-white text-black"
                        : "border-white/8 bg-white/[0.02] text-white/46 hover:bg-white/[0.055] hover:text-white",
                      !supported &&
                        item.id !== "selected" &&
                        "cursor-not-allowed opacity-25"
                    )}
                  >
                    {item.label}
                  </button>
                )
              })}
            </div>

            <SectionLabel icon={Route} label="Branch catchment" />
            <div className="grid gap-2">
              <select
                value={branchId ?? ""}
                disabled={!capabilities.branches || branches.length === 0}
                onChange={(event) =>
                  onBranchChange(event.target.value || null)
                }
                className="h-10 w-full rounded-xl border border-white/8 bg-white/[0.035] px-3 text-xs text-white outline-none disabled:cursor-not-allowed disabled:opacity-30"
              >
                <option value="">No branch selected</option>
                {branches.map((branch) => (
                  <option key={branch.id} value={branch.id}>
                    {branch.name}
                  </option>
                ))}
              </select>

              <div className="rounded-xl border border-white/8 bg-white/[0.025] p-3">
                <div className="mb-2 flex items-center justify-between text-[10px] text-white/45">
                  <span>Catchment radius</span>
                  <span className="font-mono text-white/75">{radiusKm} km</span>
                </div>
                <input
                  type="range"
                  min={1}
                  max={50}
                  step={1}
                  value={radiusKm}
                  disabled={!branchId}
                  onChange={(event) => onRadiusChange(Number(event.target.value))}
                  className="w-full accent-white disabled:opacity-25"
                />
              </div>
            </div>

            <SectionLabel icon={Activity} label="Representation handoff" />
            <div className="grid grid-cols-4 gap-1.5">
              {(["C", "R", "A", "O"] as const).map((surface) => (
                <button
                  key={surface}
                  type="button"
                  onClick={() => onHandoff(surface)}
                  className="rounded-xl border border-white/8 bg-white/[0.025] px-2 py-2 text-[10px] font-semibold text-white/45 transition hover:border-white/16 hover:bg-white/[0.06] hover:text-white"
                >
                  {surface}
                </button>
              ))}
            </div>
          </div>
        ) : null}
      </div>

      <div className="mt-2 flex items-center gap-2 px-1 text-[9px] uppercase tracking-[0.16em] text-white/24">
        <LocateFixed className="size-3" />
        Semantic LOD · GPU surface
        <Building2 className="ml-auto size-3" />
      </div>
    </aside>
  )
}

function SectionLabel({
  icon: Icon,
  label,
}: {
  icon: typeof Layers3
  label: string
}) {
  return (
    <div className="mb-2 mt-4 flex items-center gap-2 text-[9px] font-semibold uppercase tracking-[0.18em] text-white/30">
      <Icon className="size-3" />
      {label}
    </div>
  )
}

function Metric({
  label,
  value,
}: {
  label: string
  value: string
}) {
  return (
    <div className="border-r border-white/6 px-2 py-2.5 text-center last:border-r-0">
      <p className="font-mono text-[11px] text-white/78">{value}</p>
      <p className="mt-0.5 truncate text-[8px] uppercase tracking-[0.12em] text-white/25">
        {label}
      </p>
    </div>
  )
}

function formatMetric(value: number | null) {
  if (value === null) return "—"

  return new Intl.NumberFormat("en", {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value)
}
