"use client"

import * as React from "react"
import { useVirtualizer } from "@tanstack/react-virtual"
import {
  ArrowRight,
  Building2,
  Search,
  UserRound,
} from "lucide-react"

import type { BoardMatrixSnapshot } from "@/lib/relationship/types"
import {
  matrixCompanyToEntity,
  matrixPersonToEntity,
} from "@/lib/relationship/types"
import type { EntityRef } from "@/lib/intelligence/types"
import { cn } from "@/lib/utils"

const ROW_HEADER_WIDTH = 250
const COLUMN_WIDTH = 138
const ROW_HEIGHT = 52
const MAX_REGION_ENTITIES = 240

export function BoardMatrix({
  snapshot,
  synchronized,
  loading,
  errorMessage,
  companyQuery,
  personQuery,
  onCompanyQueryChange,
  onPersonQueryChange,
  onFocusCompany,
  onFocusPerson,
  onSetSelection,
  onOpenNetwork,
}: {
  snapshot: BoardMatrixSnapshot
  synchronized: boolean
  loading: boolean
  errorMessage: string | null
  companyQuery: string
  personQuery: string
  onCompanyQueryChange: (value: string) => void
  onPersonQueryChange: (value: string) => void
  onFocusCompany: (entity: EntityRef) => void
  onFocusPerson: (entity: EntityRef) => void
  onSetSelection: (entities: EntityRef[]) => void
  onOpenNetwork: () => void
}) {
  const [sortByDegree, setSortByDegree] = React.useState(true)
  const [anchor, setAnchor] = React.useState<{
    companyId: string
    personId: string
  } | null>(null)
  const scrollRef = React.useRef<HTMLDivElement>(null)

  const companies = React.useMemo(
    () =>
      sortByDegree
        ? [...snapshot.companies].sort(
            (left, right) => right.degree - left.degree
          )
        : snapshot.companies,
    [snapshot.companies, sortByDegree]
  )
  const people = React.useMemo(
    () =>
      sortByDegree
        ? [...snapshot.people].sort(
            (left, right) => right.degree - left.degree
          )
        : snapshot.people,
    [snapshot.people, sortByDegree]
  )

  const memberships = React.useMemo(
    () =>
      new Map(
        snapshot.memberships.map((membership) => [
          membership.companyId + "::" + membership.personId,
          membership,
        ])
      ),
    [snapshot.memberships]
  )

  const rowVirtualizer = useVirtualizer({
    count: companies.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 8,
  })

  const columnVirtualizer = useVirtualizer({
    horizontal: true,
    count: people.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => COLUMN_WIDTH,
    overscan: 5,
  })

  const totalWidth = ROW_HEADER_WIDTH + columnVirtualizer.getTotalSize()

  function selectCell(
    companyIndex: number,
    personIndex: number,
    shiftKey: boolean
  ) {
    const company = companies[companyIndex]
    const person = people[personIndex]
    if (!company || !person) return

    if (!shiftKey || !anchor) {
      setAnchor({ companyId: company.id, personId: person.id })
      onSetSelection([
        matrixCompanyToEntity(company),
        matrixPersonToEntity(person),
      ])
      return
    }

    const anchorCompanyIndex = companies.findIndex(
      (item) => item.id === anchor.companyId
    )
    const anchorPersonIndex = people.findIndex(
      (item) => item.id === anchor.personId
    )
    if (anchorCompanyIndex < 0 || anchorPersonIndex < 0) return

    const rowStart = Math.min(anchorCompanyIndex, companyIndex)
    const rowEnd = Math.max(anchorCompanyIndex, companyIndex)
    const columnStart = Math.min(anchorPersonIndex, personIndex)
    const columnEnd = Math.max(anchorPersonIndex, personIndex)

    const regionEntities = [
      ...companies
        .slice(rowStart, rowEnd + 1)
        .map(matrixCompanyToEntity),
      ...people
        .slice(columnStart, columnEnd + 1)
        .map(matrixPersonToEntity),
    ].slice(0, MAX_REGION_ENTITIES)

    onSetSelection(regionEntities)
  }

  return (
    <div className="absolute inset-0 flex flex-col pt-[152px] sm:pt-[148px]">
      <div className="pointer-events-auto mx-4 mb-2 flex flex-wrap items-center gap-2 sm:mx-7">
        <MatrixSearch
          icon={Building2}
          value={companyQuery}
          onChange={onCompanyQueryChange}
          placeholder="Company axis…"
        />
        <MatrixSearch
          icon={UserRound}
          value={personQuery}
          onChange={onPersonQueryChange}
          placeholder="Person axis…"
        />
        <button
          type="button"
          onClick={() => setSortByDegree((value) => !value)}
          className={cn(
            "rounded-xl border px-3 py-2 text-[9px] transition",
            sortByDegree
              ? "border-white/18 bg-white text-black"
              : "border-white/8 bg-[#081016]/82 text-white/36 hover:bg-white/[0.06]"
          )}
        >
          Degree order
        </button>
        <button
          type="button"
          onClick={onOpenNetwork}
          className="ml-auto inline-flex items-center gap-2 rounded-xl border border-white/8 bg-[#081016]/82 px-3 py-2 text-[9px] text-white/40 hover:bg-white/[0.06] hover:text-white"
        >
          Network
          <ArrowRight className="size-3" />
        </button>
      </div>

      <div className="mx-4 mb-2 flex items-center justify-between text-[8px] uppercase tracking-[0.13em] text-white/22 sm:mx-7">
        <span>
          {companies.length.toLocaleString()} companies ×{" "}
          {people.length.toLocaleString()} people ·{" "}
          {snapshot.memberships.length.toLocaleString()} memberships
        </span>
        <span>Click cell selects pair · Shift-click selects region</span>
      </div>

      <div
        ref={scrollRef}
        className="pointer-events-auto mx-4 mb-20 min-h-0 flex-1 overflow-auto rounded-2xl border border-white/8 bg-[#081016]/88 shadow-[0_24px_80px_rgba(0,0,0,.32)] backdrop-blur-xl sm:mx-7"
      >
        {!synchronized ? (
          <div className="grid h-full min-h-[420px] place-items-center px-8 text-center">
            <div>
              <LoaderMatrixState />
              <p className="mt-3 text-sm font-medium text-white/48">
                Preparing updated Matrix query
              </p>
              <p className="mx-auto mt-2 max-w-lg text-[10px] leading-5 text-white/25">
                Previous membership cells are intentionally hidden until both visible axis searches match the debounced serving query.
              </p>
            </div>
          </div>
        ) : loading ? (
          <div className="grid h-full min-h-[420px] place-items-center px-8 text-center">
            <div>
              <LoaderMatrixState />
              <p className="mt-3 text-sm font-medium text-white/48">
                Loading Board Matrix
              </p>
              <p className="mx-auto mt-2 max-w-lg text-[10px] leading-5 text-white/25">
                The axis controls remain interactive while the synchronized Matrix population is requested.
              </p>
            </div>
          </div>
        ) : errorMessage ? (
          <div className="grid h-full min-h-[420px] place-items-center px-8 text-center">
            <div>
              <p className="text-sm font-medium text-rose-100/55">
                Board Matrix request failed
              </p>
              <p className="mx-auto mt-2 max-w-lg text-[10px] leading-5 text-white/25">
                {errorMessage}
              </p>
            </div>
          </div>
        ) : companies.length === 0 || people.length === 0 ? (
          <div className="grid h-full min-h-[420px] place-items-center px-8 text-center">
            <div>
              <p className="text-sm font-medium text-white/48">
                Board Matrix unavailable
              </p>
              <p className="mx-auto mt-2 max-w-lg text-[10px] leading-5 text-white/25">
                {snapshot.message ||
                  "The serving model returned no Company × Person axes for the current Lens and search."}
              </p>
            </div>
          </div>
        ) : (
          <div
            className="relative"
            style={{
              width: totalWidth,
              minWidth: totalWidth,
            }}
          >
            <div
              className="sticky top-0 z-30 h-[76px] border-b border-white/8 bg-[#0a1117]/98"
              style={{ width: totalWidth }}
            >
              <div
                className="sticky left-0 z-40 grid h-full place-items-center border-r border-white/8 bg-[#0b1218] px-4 text-left"
                style={{ width: ROW_HEADER_WIDTH }}
              >
                <div className="w-full">
                  <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-white/31">
                    Company × Director
                  </p>
                  <p className="mt-1 text-[8px] text-white/20">
                    Frozen entity axes · virtual body
                  </p>
                </div>
              </div>

              {columnVirtualizer.getVirtualItems().map((virtualColumn) => {
                const person = people[virtualColumn.index]
                return (
                  <button
                    key={person.id}
                    type="button"
                    onClick={() => onFocusPerson(matrixPersonToEntity(person))}
                    title={person.name}
                    className="absolute top-0 flex h-full flex-col justify-end border-r border-white/6 px-2 pb-2 text-left transition hover:bg-white/[0.045]"
                    style={{
                      left: ROW_HEADER_WIDTH + virtualColumn.start,
                      width: virtualColumn.size,
                    }}
                  >
                    <span className="line-clamp-2 text-[9px] font-medium leading-3 text-white/47">
                      {person.name}
                    </span>
                    <span className="mt-1 font-mono text-[7px] text-white/19">
                      d{person.degree}
                    </span>
                  </button>
                )
              })}
            </div>

            <div
              className="relative"
              style={{ height: rowVirtualizer.getTotalSize() }}
            >
              {rowVirtualizer.getVirtualItems().map((virtualRow) => {
                const company = companies[virtualRow.index]

                return (
                  <div
                    key={company.id}
                    className="absolute left-0 top-0 border-b border-white/5"
                    style={{
                      width: totalWidth,
                      height: virtualRow.size,
                      transform: "translateY(" + virtualRow.start + "px)",
                    }}
                  >
                    <button
                      type="button"
                      onClick={() =>
                        onFocusCompany(matrixCompanyToEntity(company))
                      }
                      title={company.name}
                      className="sticky left-0 z-20 flex h-full items-center gap-2 border-r border-white/8 bg-[#0b1218]/98 px-3 text-left transition hover:bg-[#121b22]"
                      style={{ width: ROW_HEADER_WIDTH }}
                    >
                      <span className="grid size-7 shrink-0 place-items-center rounded-lg border border-white/7 bg-white/[0.025] text-[9px] font-semibold text-white/36">
                        {company.name.trim().charAt(0).toUpperCase() || "?"}
                      </span>
                      <span className="min-w-0 flex-1">
                        <span className="block truncate text-[9px] font-medium text-white/49">
                          {company.name}
                        </span>
                        <span className="mt-0.5 block font-mono text-[7px] text-white/18">
                          d{company.degree}
                        </span>
                      </span>
                    </button>

                    {columnVirtualizer.getVirtualItems().map((virtualColumn) => {
                      const person = people[virtualColumn.index]
                      const key = company.id + "::" + person.id
                      const membership = memberships.get(key)
                      const isAnchor =
                        anchor?.companyId === company.id &&
                        anchor?.personId === person.id

                      return (
                        <button
                          key={key}
                          type="button"
                          onClick={(event) =>
                            selectCell(
                              virtualRow.index,
                              virtualColumn.index,
                              event.shiftKey
                            )
                          }
                          title={
                            membership
                              ? membership.role ||
                                company.name + " × " + person.name
                              : "No membership"
                          }
                          className={cn(
                            "absolute top-0 grid h-full place-items-center border-r border-white/5 transition hover:bg-white/[0.045]",
                            isAnchor && "bg-emerald-200/[0.075]"
                          )}
                          style={{
                            left: ROW_HEADER_WIDTH + virtualColumn.start,
                            width: virtualColumn.size,
                          }}
                        >
                          {membership ? (
                            <span
                              className={cn(
                                "size-2 rounded-full border",
                                company.tipStatus === "active"
                                  ? "border-cyan-100/35 bg-cyan-200/55"
                                  : company.tipStatus === "expired"
                                    ? "border-rose-100/28 bg-rose-200/45"
                                    : "border-white/30 bg-white/42"
                              )}
                            />
                          ) : (
                            <span className="size-1 rounded-full bg-white/[0.045]" />
                          )}
                        </button>
                      )
                    })}
                  </div>
                )
              })}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

function MatrixSearch({
  icon: Icon,
  value,
  onChange,
  placeholder,
}: {
  icon: typeof Search
  value: string
  onChange: (value: string) => void
  placeholder: string
}) {
  return (
    <label className="relative block min-w-[180px] flex-1 sm:max-w-xs">
      <Icon className="pointer-events-none absolute left-3 top-1/2 size-3.5 -translate-y-1/2 text-white/24" />
      <input
        value={value}
        onChange={(event) => onChange(event.target.value)}
        placeholder={placeholder}
        data-keyboard-input
        className="h-9 w-full rounded-xl border border-white/8 bg-[#081016]/82 pl-9 pr-3 text-[10px] text-white outline-none placeholder:text-white/20 focus:border-white/15 focus:bg-[#0b141b]"
      />
    </label>
  )
}


function LoaderMatrixState() {
  return (
    <span className="mx-auto block size-5 animate-pulse rounded-full border border-white/15 bg-white/[0.04]" />
  )
}
