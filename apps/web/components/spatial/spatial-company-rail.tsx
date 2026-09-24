"use client"

import * as React from "react"
import { useVirtualizer } from "@tanstack/react-virtual"
import {
  Building2,
  ChevronDown,
  ChevronUp,
  Crosshair,
  Layers3,
} from "lucide-react"

import type { SpatialCompanyPoint } from "@/lib/spatial/types"
import { cn } from "@/lib/utils"

export function SpatialCompanyRail({
  companies,
  open,
  hoveredCompanyId,
  selectedCompanyIds,
  onOpenChange,
  onHoverCompany,
  onFocusCompany,
  onToggleSelection,
}: {
  companies: SpatialCompanyPoint[]
  open: boolean
  hoveredCompanyId: string | null
  selectedCompanyIds: ReadonlySet<string>
  onOpenChange: (open: boolean) => void
  onHoverCompany: (company: SpatialCompanyPoint | null) => void
  onFocusCompany: (company: SpatialCompanyPoint) => void
  onToggleSelection: (company: SpatialCompanyPoint) => void
}) {
  const scrollRef = React.useRef<HTMLDivElement>(null)
  const virtualizer = useVirtualizer({
    count: companies.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => 62,
    overscan: 8,
  })

  return (
    <aside className="pointer-events-auto fixed bottom-20 right-5 z-[38] w-[min(360px,calc(100vw-40px))] sm:right-7">
      <div className="overflow-hidden rounded-2xl border border-white/10 bg-[#081016]/90 shadow-[0_24px_80px_rgba(0,0,0,.38)] backdrop-blur-2xl">
        <button
          type="button"
          onClick={() => onOpenChange(!open)}
          className="flex w-full items-center gap-3 px-4 py-3 text-left"
          aria-expanded={open}
        >
          <span className="grid size-8 place-items-center rounded-lg border border-white/10 bg-white/[0.05] text-white/65">
            <Layers3 className="size-3.5" />
          </span>
          <span className="min-w-0 flex-1">
            <span className="block text-[10px] font-semibold uppercase tracking-[0.17em] text-white/58">
              Visible company population
            </span>
            <span className="mt-0.5 block text-[10px] text-white/30">
              {companies.length > 0
                ? `${companies.length.toLocaleString()} loaded entities`
                : "No precise company geometry in current response"}
            </span>
          </span>
          {open ? (
            <ChevronDown className="size-4 text-white/30" />
          ) : (
            <ChevronUp className="size-4 text-white/30" />
          )}
        </button>

        {open ? (
          <div
            ref={scrollRef}
            className="h-[min(42vh,420px)] overflow-auto border-t border-white/8"
          >
            {companies.length === 0 ? (
              <div className="grid h-full place-items-center px-6 text-center">
                <div>
                  <Building2 className="mx-auto size-5 text-white/20" />
                  <p className="mt-3 text-xs font-medium text-white/50">
                    Company geometry unavailable
                  </p>
                  <p className="mx-auto mt-1 max-w-64 text-[10px] leading-4 text-white/28">
                    This rail only displays production company coordinates from
                    the Center Database spatial serving contract.
                  </p>
                </div>
              </div>
            ) : (
              <div
                className="relative w-full"
                style={{ height: virtualizer.getTotalSize() }}
              >
                {virtualizer.getVirtualItems().map((virtualRow) => {
                  const company = companies[virtualRow.index]
                  const hovered = hoveredCompanyId === company.id
                  const selected = selectedCompanyIds.has(company.id)

                  return (
                    <div
                      key={company.id}
                      data-index={virtualRow.index}
                      ref={virtualizer.measureElement}
                      className="absolute left-0 top-0 w-full px-2 py-1"
                      style={{
                        transform: `translateY(${virtualRow.start}px)`,
                      }}
                    >
                      <div
                        onMouseEnter={() => onHoverCompany(company)}
                        onMouseLeave={() => onHoverCompany(null)}
                        className={cn(
                          "flex items-center gap-2 rounded-xl border px-2.5 py-2 transition",
                          hovered
                            ? "border-white/18 bg-white/[0.075]"
                            : "border-transparent bg-transparent hover:bg-white/[0.04]"
                        )}
                      >
                        <button
                          type="button"
                          onClick={() => onFocusCompany(company)}
                          className="min-w-0 flex-1 text-left"
                        >
                          <p className="truncate text-[11px] font-medium text-white/72">
                            {company.name}
                          </p>
                          <p className="mt-0.5 truncate text-[9px] text-white/28">
                            {[company.businessType, company.district, company.province]
                              .filter(Boolean)
                              .join(" · ") || "Company entity"}
                          </p>
                        </button>
                        <button
                          type="button"
                          onClick={() => onToggleSelection(company)}
                          aria-label={
                            selected
                              ? `Remove ${company.name} from selection`
                              : `Add ${company.name} to selection`
                          }
                          className={cn(
                            "grid size-8 shrink-0 place-items-center rounded-lg border transition",
                            selected
                              ? "border-emerald-200/35 bg-emerald-200/12 text-emerald-100"
                              : "border-white/8 bg-white/[0.025] text-white/28 hover:text-white/70"
                          )}
                        >
                          <Crosshair className="size-3.5" />
                        </button>
                      </div>
                    </div>
                  )
                })}
              </div>
            )}
          </div>
        ) : null}
      </div>
    </aside>
  )
}
