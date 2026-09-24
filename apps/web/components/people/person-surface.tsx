"use client"

import * as React from "react"
import { useQuery, type UseQueryResult } from "@tanstack/react-query"
import {
  AlertTriangle,
  Database,
  LoaderCircle,
  Search,
  UserRound,
  X,
} from "lucide-react"
import { motion, useReducedMotion } from "motion/react"

import { CompanyBoard } from "./company-board"
import { PersonDossier } from "./person-dossier"
import { useGlobalLens } from "@/hooks/use-global-lens"
import {
  fetchCompanyBoard,
  fetchPersonDetail,
  relationshipEndpointConfigured,
  searchPeople,
} from "@/lib/relationship/adapter"
import { resolvePeopleSurfaceSnapshot } from "@/lib/relationship/surface-snapshot"
import {
  personSearchResultToEntity,
  type PersonSearchResult,
  type PersonSearchSnapshot,
} from "@/lib/relationship/types"
import type { EntityRef } from "@/lib/intelligence/types"
import { useWorkspaceStore } from "@/stores/workspace-store"

export function PersonSurface() {
  const reduceMotion = useReducedMotion()
  const { lens, activeCount } = useGlobalLens()
  const focus = useWorkspaceStore((state) => state.focus)
  const setFocus = useWorkspaceStore((state) => state.setFocus)
  const setActiveSurface = useWorkspaceStore((state) => state.setActiveSurface)
  const saveSurfaceSnapshot = useWorkspaceStore(
    (state) => state.saveSurfaceSnapshot
  )
  const snapshotPayload = useWorkspaceStore(
    (state) => state.surfaceSnapshots.P?.payload
  )

  const focusedPerson = focus?.kind === "person" ? focus : null
  const focusedCompany = focus?.kind === "company" ? focus : null
  const focusedPersonId = focusedPerson?.id ?? null

  const initialState = React.useMemo(
    () => resolvePeopleSurfaceSnapshot(snapshotPayload, focusedPersonId),
    [focusedPersonId, snapshotPayload]
  )

  const [searchTerm, setSearchTerm] = React.useState(initialState.searchTerm)
  const [debouncedSearch, setDebouncedSearch] = React.useState(
    initialState.searchTerm
  )

  React.useEffect(() => {
    const timeout = window.setTimeout(
      () => setDebouncedSearch(searchTerm.trim()),
      320
    )
    return () => window.clearTimeout(timeout)
  }, [searchTerm])

  React.useEffect(() => {
    saveSurfaceSnapshot("P", {
      focusedPersonId,
      searchTerm,
    })
  }, [focusedPersonId, saveSurfaceSnapshot, searchTerm])

  const searchQuery = useQuery({
    queryKey: ["tipx", "people", "search", debouncedSearch, lens],
    queryFn: ({ signal }) => searchPeople(debouncedSearch, lens, signal),
    enabled: debouncedSearch.length >= 2,
    staleTime: 30_000,
    retry: 1,
  })

  const personQuery = useQuery({
    queryKey: ["tipx", "people", "detail", focusedPerson?.id, lens],
    queryFn: ({ signal }) =>
      fetchPersonDetail(focusedPerson!.id, lens, signal),
    enabled: focusedPerson !== null,
    staleTime: 60_000,
    retry: 1,
  })

  const boardQuery = useQuery({
    queryKey: ["tipx", "people", "company-board", focusedCompany?.id, lens],
    queryFn: ({ signal }) =>
      fetchCompanyBoard(focusedCompany!.id, lens, signal),
    enabled: focusedCompany !== null,
    staleTime: 60_000,
    retry: 1,
  })

  function focusEntity(entity: EntityRef) {
    setFocus(entity)
    setSearchTerm("")
  }

  function handoffRelationship(view: "explore" | "matrix") {
    saveSurfaceSnapshot("R", {
      view,
      depth: 1,
      matrixCompanyQuery: "",
      matrixPersonQuery: "",
    })
    setActiveSurface("R")
  }

  return (
    <main className="relative min-h-svh overflow-x-hidden bg-tipx-canvas text-tipx-ink">
      <div className="tipx-grid fixed inset-0 opacity-24" />
      <div className="tipx-vignette fixed inset-0" />
      <div className="pointer-events-none fixed left-[14%] top-[10%] size-[40vw] max-h-[720px] max-w-[720px] rounded-full bg-cyan-200/[0.012] blur-3xl" />

      <PeopleCommandBar
        searchTerm={searchTerm}
        debouncedSearch={debouncedSearch}
        onSearchTermChange={setSearchTerm}
        focus={focusedPerson ?? focusedCompany}
        searchQuery={searchQuery}
        onPickPerson={(person) => focusEntity(personSearchResultToEntity(person))}
        onClearFocus={() => setFocus(null)}
      />

      <div className="relative z-10 mx-auto w-full max-w-[1680px] px-4 pb-36 pt-32 sm:px-7 lg:px-10 lg:pt-36">
        {focusedPerson ? (
          personQuery.isPending ? (
            <SurfaceState
              icon={LoaderCircle}
              title="Resolving person identity"
              spinning
            />
          ) : personQuery.isError ? (
            <SurfaceState
              icon={AlertTriangle}
              title="Person serving request failed"
              detail={
                personQuery.error instanceof Error
                  ? personQuery.error.message
                  : "Person serving request failed."
              }
            />
          ) : personQuery.data ? (
            <motion.div
              key={focusedPerson.id}
              initial={reduceMotion ? false : { opacity: 0, y: 8 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.18 }}
            >
              <PersonDossier
                detail={personQuery.data}
                fallbackEntity={focusedPerson}
                onFocus={(entity) => {
                  focusEntity(entity)
                  if (entity.kind === "company") setActiveSurface("C")
                }}
                onHandoff={setActiveSurface}
                onOpenMatrix={() => handoffRelationship("matrix")}
              />
            </motion.div>
          ) : null
        ) : focusedCompany ? (
          boardQuery.isPending ? (
            <SurfaceState
              icon={LoaderCircle}
              title="Resolving company board"
              spinning
            />
          ) : boardQuery.isError ? (
            <SurfaceState
              icon={AlertTriangle}
              title="Company-board request failed"
              detail={
                boardQuery.error instanceof Error
                  ? boardQuery.error.message
                  : "Company-board request failed."
              }
            />
          ) : boardQuery.data ? (
            <CompanyBoard
              snapshot={boardQuery.data}
              fallbackCompany={focusedCompany}
              onFocusPerson={(person) => focusEntity(person)}
              onOpenRelationship={() => handoffRelationship("explore")}
              onOpenMatrix={() => handoffRelationship("matrix")}
              onOpenCompany={() => setActiveSurface("C")}
            />
          ) : null
        ) : (
          <PeopleDiscovery
            endpointConfigured={relationshipEndpointConfigured()}
            activeLensCount={activeCount}
          />
        )}
      </div>
    </main>
  )
}

function PeopleCommandBar({
  searchTerm,
  debouncedSearch,
  onSearchTermChange,
  focus,
  searchQuery,
  onPickPerson,
  onClearFocus,
}: {
  searchTerm: string
  debouncedSearch: string
  onSearchTermChange: (value: string) => void
  focus: EntityRef | null
  searchQuery: UseQueryResult<PersonSearchSnapshot, Error>
  onPickPerson: (person: PersonSearchResult) => void
  onClearFocus: () => void
}) {
  const normalized = searchTerm.trim()
  const showResults = normalized.length >= 2
  const synchronized = showResults && normalized === debouncedSearch
  const results = synchronized ? searchQuery.data?.results ?? [] : []

  return (
    <div className="pointer-events-none fixed left-4 right-[4.75rem] top-4 z-[35] flex items-start gap-2 sm:left-7 sm:right-24 sm:top-7">
      <div className="pointer-events-auto flex min-w-0 flex-1 items-center gap-2 rounded-2xl border border-tipx-line/10 bg-tipx-floating/91 p-1.5 shadow-[0_24px_80px_rgba(0,0,0,.35)] backdrop-blur-2xl sm:max-w-2xl">
        <span className="grid size-10 shrink-0 place-items-center rounded-xl bg-tipx-brand font-mono text-xs font-bold text-tipx-on-brand">
          P
        </span>
        <div className="relative min-w-0 flex-1">
          <Search className="pointer-events-none absolute left-3 top-1/2 size-3.5 -translate-y-1/2 text-tipx-ink/25" />
          <input
            value={searchTerm}
            onChange={(event) => onSearchTermChange(event.target.value)}
            placeholder="Search director / person identity…"
            data-keyboard-input
            className="h-10 w-full rounded-xl border border-transparent bg-tipx-ink/[0.025] pl-9 pr-9 text-xs text-tipx-ink outline-none transition placeholder:text-tipx-ink/21 focus:border-tipx-line/10 focus:bg-tipx-ink/[0.045]"
          />
          {searchTerm ? (
            <button
              type="button"
              onClick={() => onSearchTermChange("")}
              className="absolute right-2 top-1/2 grid size-7 -translate-y-1/2 place-items-center rounded-lg text-tipx-ink/24 hover:bg-tipx-ink/6 hover:text-tipx-ink"
              aria-label="Clear person search"
            >
              <X className="size-3.5" />
            </button>
          ) : null}

          {showResults ? (
            <PersonSearchPopover
              synchronized={synchronized}
              queryState={searchQuery}
              results={results}
              onPickPerson={onPickPerson}
            />
          ) : null}
        </div>
      </div>

      {focus ? (
        <button
          type="button"
          onClick={onClearFocus}
          className="pointer-events-auto grid size-[52px] shrink-0 place-items-center rounded-2xl border border-tipx-line/9 bg-tipx-floating/86 text-tipx-ink/38 backdrop-blur-xl transition hover:bg-tipx-ink/[0.06] hover:text-tipx-ink"
          aria-label={"Clear focused " + focus.kind + ": " + focus.label}
          title="Return to people discovery"
        >
          <X className="size-3.5" />
        </button>
      ) : null}
    </div>
  )
}

function PersonSearchPopover({
  synchronized,
  queryState,
  results,
  onPickPerson,
}: {
  synchronized: boolean
  queryState: UseQueryResult<PersonSearchSnapshot, Error>
  results: PersonSearchResult[]
  onPickPerson: (person: PersonSearchResult) => void
}) {
  return (
    <div className="absolute left-0 right-0 top-[calc(100%+10px)] overflow-hidden rounded-2xl border border-tipx-line/10 bg-[#091117]/98 shadow-[0_28px_90px_rgba(0,0,0,.55)] backdrop-blur-2xl">
      {!synchronized ? (
        <SearchState
          icon={LoaderCircle}
          title="Preparing updated query"
          detail="Previous person results are hidden until the debounced query matches the visible text."
        />
      ) : queryState.isPending ? (
        <SearchState
          icon={LoaderCircle}
          title="Searching person identities"
          detail="Resolving canonical people through the relationship serving model."
          spinning
        />
      ) : queryState.isError ? (
        <SearchState
          icon={AlertTriangle}
          title="Person search failed"
          detail={
            queryState.error instanceof Error
              ? queryState.error.message
              : "Person search failed."
          }
        />
      ) : queryState.data?.availability === "unavailable" ? (
        <SearchState
          icon={Database}
          title="Relationship serving unavailable"
          detail={
            queryState.data.message || "No production person-search endpoint."
          }
        />
      ) : results.length > 0 ? (
        <div className="max-h-[420px] overflow-auto p-1.5">
          {results.map((person) => (
            <button
              key={person.id}
              type="button"
              onClick={() => onPickPerson(person)}
              className="flex w-full items-center gap-3 rounded-xl px-3 py-3 text-left transition hover:bg-tipx-ink/[0.055]"
            >
              <span className="grid size-9 shrink-0 place-items-center rounded-xl border border-tipx-line/8 bg-tipx-ink/[0.03]">
                <UserRound className="size-3.5 text-tipx-ink/35" />
              </span>
              <span className="min-w-0 flex-1">
                <span className="block truncate text-[11px] font-medium text-tipx-ink/68">
                  {person.name}
                </span>
                <span className="mt-0.5 block truncate text-[9px] text-tipx-ink/27">
                  {person.secondary ||
                    (person.companyCount == null
                      ? "Company context unavailable"
                      : person.companyCount + " companies")}
                </span>
              </span>
              <span className="font-mono text-[8px] text-tipx-ink/23">
                {person.companyCount ?? "—"}
              </span>
            </button>
          ))}
        </div>
      ) : (
        <SearchState
          icon={Search}
          title="No matching person"
          detail="The relationship serving model returned no person identity for this query and Lens."
        />
      )}
    </div>
  )
}

function PeopleDiscovery({
  endpointConfigured,
  activeLensCount,
}: {
  endpointConfigured: boolean
  activeLensCount: number
}) {
  return (
    <section className="grid min-h-[calc(100svh-12rem)] place-items-center">
      <div className="max-w-2xl text-center">
        <span className="mx-auto grid size-12 place-items-center rounded-2xl border border-tipx-line/9 bg-tipx-ink/[0.025]">
          <UserRound className="size-5 text-tipx-ink/38" />
        </span>
        <p className="mt-6 font-mono text-[10px] uppercase tracking-[0.28em] text-tipx-ink/24">
          P / People Intelligence
        </p>
        <h1 className="mt-3 text-balance text-4xl font-semibold tracking-[-0.045em] text-tipx-ink sm:text-6xl">
          Follow the person,
          <br />
          preserve the network.
        </h1>
        <p className="mx-auto mt-5 max-w-xl text-pretty text-[12px] leading-6 text-tipx-ink/34">
          Search a canonical director identity or arrive from a company. Held
          companies remain an entity field here; multi-hop structure changes
          representation in Relationship and Matrix without losing context.
        </p>
        <div className="mx-auto mt-7 flex w-fit items-center gap-4 rounded-2xl border border-tipx-line/8 bg-tipx-ink/[0.02] px-4 py-3 text-[9px] uppercase tracking-[0.13em] text-tipx-ink/27">
          <span className="inline-flex items-center gap-2">
            <span
              className={
                endpointConfigured
                  ? "size-1.5 rounded-full bg-emerald-300/70"
                  : "size-1.5 rounded-full bg-tipx-ink/16"
              }
            />
            {endpointConfigured ? "Serving configured" : "Serving unavailable"}
          </span>
          <span className="h-3 w-px bg-tipx-ink/8" />
          <span>Lens {activeLensCount}</span>
        </div>
      </div>
    </section>
  )
}

function SurfaceState({
  icon: Icon,
  title,
  detail,
  spinning = false,
}: {
  icon: typeof Search
  title: string
  detail?: string
  spinning?: boolean
}) {
  return (
    <div className="grid min-h-[calc(100svh-13rem)] place-items-center text-center">
      <div>
        <Icon
          className={
            "mx-auto size-6 text-tipx-ink/28 " + (spinning ? "animate-spin" : "")
          }
        />
        <p className="mt-4 text-sm font-medium text-tipx-ink/52">{title}</p>
        {detail ? (
          <p className="mx-auto mt-2 max-w-lg text-[11px] leading-5 text-tipx-ink/28">
            {detail}
          </p>
        ) : null}
      </div>
    </div>
  )
}

function SearchState({
  icon: Icon,
  title,
  detail,
  spinning = false,
}: {
  icon: typeof Search
  title: string
  detail: string
  spinning?: boolean
}) {
  return (
    <div className="flex items-start gap-3 p-4">
      <span className="grid size-8 shrink-0 place-items-center rounded-xl border border-tipx-line/8 bg-tipx-ink/[0.025]">
        <Icon
          className={
            "size-3.5 text-tipx-ink/30 " + (spinning ? "animate-spin" : "")
          }
        />
      </span>
      <span>
        <span className="block text-[11px] font-medium text-tipx-ink/55">
          {title}
        </span>
        <span className="mt-1 block text-[9px] leading-4 text-tipx-ink/27">
          {detail}
        </span>
      </span>
    </div>
  )
}
