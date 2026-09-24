"use client"

import * as React from "react"
import { useVirtualizer } from "@tanstack/react-virtual"
import {
  ArrowUpRight,
  Building2,
  Network,
  UserRound,
  UsersRound,
} from "lucide-react"

import type { CompanyBoardSnapshot } from "@/lib/relationship/types"
import type { EntityRef } from "@/lib/intelligence/types"

export function CompanyBoard({
  snapshot,
  fallbackCompany,
  onFocusPerson,
  onOpenRelationship,
  onOpenMatrix,
  onOpenCompany,
}: {
  snapshot: CompanyBoardSnapshot
  fallbackCompany: EntityRef
  onFocusPerson: (person: EntityRef) => void
  onOpenRelationship: () => void
  onOpenMatrix: () => void
  onOpenCompany: () => void
}) {
  const company = snapshot.company ?? fallbackCompany
  const scrollRef = React.useRef<HTMLDivElement>(null)
  const virtualizer = useVirtualizer({
    count: snapshot.members.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => 66,
    overscan: 8,
  })

  return (
    <section className="rounded-[28px] border border-white/8 bg-[#0a1015]/76 p-6 backdrop-blur-2xl sm:p-7">
      <div className="flex flex-col gap-5 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <p className="font-mono text-[9px] uppercase tracking-[0.22em] text-white/24">
            Company → board population
          </p>
          <div className="mt-3 flex items-start gap-4">
            <span className="grid size-12 place-items-center rounded-2xl border border-white/9 bg-white/[0.035]">
              <Building2 className="size-5 text-white/38" />
            </span>
            <div>
              <h1 className="text-balance text-3xl font-semibold tracking-[-0.04em] text-white sm:text-4xl">
                {company.label}
              </h1>
              <p className="mt-2 text-[11px] text-white/30">
                {company.secondary || "Canonical company board context"}
              </p>
            </div>
          </div>
        </div>

        <div className="flex gap-1.5">
          <button
            type="button"
            onClick={onOpenCompany}
            className="rounded-xl border border-white/8 bg-white/[0.025] px-3 py-2 text-[9px] text-white/43 transition hover:bg-white/[0.06] hover:text-white"
          >
            C · Company
          </button>
          <button
            type="button"
            onClick={onOpenRelationship}
            className="rounded-xl border border-white/8 bg-white/[0.025] px-3 py-2 text-[9px] text-white/43 transition hover:bg-white/[0.06] hover:text-white"
          >
            R · Network
          </button>
          <button
            type="button"
            onClick={onOpenMatrix}
            className="rounded-xl border border-white/8 bg-white/[0.025] px-3 py-2 text-[9px] text-white/43 transition hover:bg-white/[0.06] hover:text-white"
          >
            Matrix
          </button>
        </div>
      </div>

      <div className="mt-7 flex items-center justify-between border-y border-white/7 py-3">
        <div className="flex items-center gap-2 text-[9px] uppercase tracking-[0.15em] text-white/25">
          <UsersRound className="size-3.5" />
          Loaded board population
        </div>
        <span className="font-mono text-sm text-white/61">
          {snapshot.members.length.toLocaleString()}
        </span>
      </div>

      {snapshot.members.length > 0 ? (
        <div
          ref={scrollRef}
          className="mt-5 h-[min(54vh,560px)] overflow-auto rounded-2xl border border-white/7 bg-black/10"
        >
          <div
            className="relative w-full"
            style={{ height: virtualizer.getTotalSize() }}
          >
            {virtualizer.getVirtualItems().map((virtualRow) => {
              const member = snapshot.members[virtualRow.index]

              return (
                <button
                  key={member.person.id + ":" + virtualRow.index}
                  ref={virtualizer.measureElement}
                  data-index={virtualRow.index}
                  type="button"
                  onClick={() => onFocusPerson(member.person)}
                  className="absolute left-0 top-0 flex w-full items-center gap-3 border-b border-white/6 px-4 py-3 text-left transition hover:bg-white/[0.045]"
                  style={{
                    transform: "translateY(" + virtualRow.start + "px)",
                  }}
                >
                  <span className="grid size-9 shrink-0 place-items-center rounded-xl border border-white/8 bg-white/[0.025]">
                    <UserRound className="size-3.5 text-white/36" />
                  </span>
                  <span className="min-w-0 flex-1">
                    <span className="block truncate text-[11px] font-medium text-white/64">
                      {member.person.label}
                    </span>
                    <span className="mt-0.5 block truncate text-[9px] text-white/26">
                      {[member.role, member.appointedAt, member.endedAt]
                        .filter(Boolean)
                        .join(" · ") || "Board membership"}
                    </span>
                  </span>
                  {member.sharedCompanyCount != null ? (
                    <span className="font-mono text-[9px] text-white/28">
                      shared ×{member.sharedCompanyCount}
                    </span>
                  ) : null}
                  <ArrowUpRight className="size-3.5 text-white/18" />
                </button>
              )
            })}
          </div>
        </div>
      ) : (
        <div className="mt-5 rounded-2xl border border-dashed border-white/8 px-5 py-12 text-center">
          <Network className="mx-auto size-5 text-white/17" />
          <p className="mt-3 text-[11px] font-medium text-white/45">
            Board population unavailable
          </p>
          <p className="mx-auto mt-1 max-w-md text-[9px] leading-4 text-white/24">
            {snapshot.message ||
              "No board-member identities were returned for this company and Lens."}
          </p>
        </div>
      )}
    </section>
  )
}
