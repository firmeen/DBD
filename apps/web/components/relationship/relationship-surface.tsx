"use client"

import * as React from "react"
import dynamic from "next/dynamic"
import { useQuery } from "@tanstack/react-query"
import Fuse from "fuse.js"
import {
  AlertTriangle,
  Anchor,
  Database,
  EyeOff,
  Focus,
  GitCompare,
  GitFork,
  LayoutGrid,
  LoaderCircle,
  Map as MapIcon,
  Network,
  Pin,
  Search,
  UserRound,
  X,
} from "lucide-react"

import { useGlobalLens } from "@/hooks/use-global-lens"
import {
  fetchBoardMatrix,
  fetchGenealogy,
  fetchRelationshipGraph,
  relationshipEndpointConfigured,
} from "@/lib/relationship/adapter"
import {
  pathEdgeIds,
  shortestRelationshipPath,
} from "@/lib/relationship/graph-algorithms"
import {
  defaultRelationshipSurfaceSnapshot,
  parseRelationshipSurfaceSnapshot,
} from "@/lib/relationship/surface-snapshot"
import type {
  BoardMatrixSnapshot,
  GenealogySnapshot,
  RelationshipGraphSnapshot,
  RelationshipNode,
  RelationshipViewId,
} from "@/lib/relationship/types"
import { relationshipNodeToEntity } from "@/lib/relationship/types"
import type { EntityRef, SurfaceId } from "@/lib/intelligence/types"
import { cn } from "@/lib/utils"
import { useWorkspaceStore } from "@/stores/workspace-store"

const RelationshipNetwork = dynamic(
  () =>
    import("./relationship-network").then(
      (module) => module.RelationshipNetwork
    ),
  {
    ssr: false,
    loading: () => <EngineLoader label="Loading Sigma / Graphology" />,
  }
)

const RelationshipNeighbourhood = dynamic(
  () =>
    import("./relationship-neighbourhood").then(
      (module) => module.RelationshipNeighbourhood
    ),
  {
    ssr: false,
    loading: () => <EngineLoader label="Loading Cytoscape / fCoSE" />,
  }
)

const BoardMatrix = dynamic(
  () => import("./board-matrix").then((module) => module.BoardMatrix),
  {
    ssr: false,
    loading: () => <EngineLoader label="Loading virtual Board Matrix" />,
  }
)

const RelationshipGenealogy = dynamic(
  () =>
    import("./relationship-genealogy").then(
      (module) => module.RelationshipGenealogy
    ),
  {
    ssr: false,
    loading: () => <EngineLoader label="Loading directed genealogy" />,
  }
)

const EMPTY_GRAPH: RelationshipGraphSnapshot = {
  availability: "unavailable",
  generatedAt: null,
  message: "Relationship graph has not been requested.",
  layout: "client",
  nodes: [],
  edges: [],
}

const EMPTY_MATRIX: BoardMatrixSnapshot = {
  availability: "unavailable",
  generatedAt: null,
  message: "Board Matrix has not been requested.",
  companies: [],
  people: [],
  memberships: [],
}

const EMPTY_GENEALOGY: GenealogySnapshot = {
  availability: "unavailable",
  generatedAt: null,
  message: "Focus a company to request directed genealogy.",
  nodes: [],
  edges: [],
}

const VIEW_META: Array<{
  id: RelationshipViewId
  short: string
  label: string
  icon: typeof Network
}> = [
  { id: "explore", short: "E", label: "Explore", icon: Network },
  { id: "neighbourhood", short: "N", label: "Neighbourhood", icon: Focus },
  { id: "matrix", short: "X", label: "Matrix", icon: LayoutGrid },
  { id: "genealogy", short: "G", label: "Genealogy", icon: GitFork },
]

export function RelationshipSurface() {
  const { lens } = useGlobalLens()
  const focus = useWorkspaceStore((state) => state.focus)
  const selection = useWorkspaceStore((state) => state.selection)
  const compare = useWorkspaceStore((state) => state.compare)
  const population = useWorkspaceStore((state) => state.population)
  const setFocus = useWorkspaceStore((state) => state.setFocus)
  const setSelection = useWorkspaceStore((state) => state.setSelection)
  const toggleSelection = useWorkspaceStore((state) => state.toggleSelection)
  const toggleCompare = useWorkspaceStore((state) => state.toggleCompare)
  const setActiveSurface = useWorkspaceStore((state) => state.setActiveSurface)
  const saveSurfaceSnapshot = useWorkspaceStore(
    (state) => state.saveSurfaceSnapshot
  )
  const snapshotPayload = useWorkspaceStore(
    (state) => state.surfaceSnapshots.R?.payload
  )

  const initialState = React.useMemo(
    () =>
      snapshotPayload
        ? parseRelationshipSurfaceSnapshot(snapshotPayload)
        : defaultRelationshipSurfaceSnapshot(),
    [snapshotPayload]
  )

  const [view, setView] = React.useState<RelationshipViewId>(initialState.view)
  const [depth, setDepth] = React.useState(initialState.depth)
  const [anchorEntity, setAnchorEntity] = React.useState<EntityRef | null>(
    () =>
      relationshipEntity(focus) ??
      relationshipEntity(
        selection.find(
          (entity) => entity.kind === "company" || entity.kind === "person"
        ) ?? null
      )
  )
  const [populationSelection, setPopulationSelection] = React.useState<
    EntityRef[]
  >(() => relationshipPopulation(selection))
  const [hiddenNodeIds, setHiddenNodeIds] = React.useState<Set<string>>(
    () => new Set()
  )
  const [pinnedNodeIds, setPinnedNodeIds] = React.useState<Set<string>>(
    () => new Set()
  )
  const [hoverEntity, setHoverEntity] = React.useState<EntityRef | null>(null)
  const [layoutRunning, setLayoutRunning] = React.useState(false)
  const [graphSearch, setGraphSearch] = React.useState("")
  const [matrixCompanyQuery, setMatrixCompanyQuery] = React.useState(
    initialState.matrixCompanyQuery
  )
  const [matrixPersonQuery, setMatrixPersonQuery] = React.useState(
    initialState.matrixPersonQuery
  )
  const [debouncedCompanyQuery, setDebouncedCompanyQuery] = React.useState(
    initialState.matrixCompanyQuery
  )
  const [debouncedPersonQuery, setDebouncedPersonQuery] = React.useState(
    initialState.matrixPersonQuery
  )

  React.useEffect(() => {
    saveSurfaceSnapshot("R", {
      view,
      depth,
      matrixCompanyQuery,
      matrixPersonQuery,
    })
  }, [
    depth,
    matrixCompanyQuery,
    matrixPersonQuery,
    saveSurfaceSnapshot,
    view,
  ])

  React.useEffect(() => {
    const timeout = window.setTimeout(() => {
      setDebouncedCompanyQuery(matrixCompanyQuery.trim())
      setDebouncedPersonQuery(matrixPersonQuery.trim())
    }, 340)
    return () => window.clearTimeout(timeout)
  }, [matrixCompanyQuery, matrixPersonQuery])

  const graphQuery = useQuery({
    queryKey: [
      "tipx",
      "relationship",
      "graph",
      anchorEntity,
      populationSelection,
      population,
      depth,
      lens,
    ],
    queryFn: ({ signal }) =>
      fetchRelationshipGraph(
        anchorEntity,
        populationSelection,
        population,
        depth,
        lens,
        signal
      ),
    enabled: view === "explore" || view === "neighbourhood",
    staleTime: 45_000,
    retry: 1,
  })

  const matrixQuery = useQuery({
    queryKey: [
      "tipx",
      "relationship",
      "matrix",
      focus,
      populationSelection,
      population,
      debouncedCompanyQuery,
      debouncedPersonQuery,
      lens,
    ],
    queryFn: ({ signal }) =>
      fetchBoardMatrix(
        relationshipEntity(focus),
        populationSelection,
        population,
        debouncedCompanyQuery,
        debouncedPersonQuery,
        lens,
        signal
      ),
    enabled: view === "matrix",
    staleTime: 45_000,
    retry: 1,
  })

  const genealogyQuery = useQuery({
    queryKey: [
      "tipx",
      "relationship",
      "genealogy",
      focus?.kind === "company" ? focus.id : null,
      depth,
      lens,
    ],
    queryFn: ({ signal }) =>
      fetchGenealogy(focus!.id, depth, lens, signal),
    enabled: view === "genealogy" && focus?.kind === "company",
    staleTime: 60_000,
    retry: 1,
  })

  const matrixSynchronized =
    matrixCompanyQuery.trim() === debouncedCompanyQuery &&
    matrixPersonQuery.trim() === debouncedPersonQuery

  const graph = graphQuery.data ?? EMPTY_GRAPH
  const matrix = matrixQuery.data ?? EMPTY_MATRIX
  const genealogy = genealogyQuery.data ?? EMPTY_GENEALOGY

  const nodeById = React.useMemo(
    () => new Map(graph.nodes.map((node) => [node.id, node])),
    [graph.nodes]
  )

  const graphFuse = React.useMemo(
    () =>
      new Fuse(graph.nodes, {
        keys: ["label", "secondary"],
        threshold: 0.28,
        ignoreLocation: true,
      }),
    [graph.nodes]
  )

  const searchResults = React.useMemo(() => {
    const query = graphSearch.trim()
    if (!query) return []
    return graphFuse
      .search(query, { limit: 8 })
      .map((result) => result.item)
  }, [graphFuse, graphSearch])

  const compareTarget = React.useMemo(
    () =>
      compare.find(
        (entity) =>
          (entity.kind === "company" || entity.kind === "person") &&
          entity.id !== focus?.id &&
          nodeById.has(entity.id)
      ) ?? null,
    [compare, focus?.id, nodeById]
  )

  const shortestPath = React.useMemo(() => {
    if (!focus || !compareTarget) return []
    return shortestRelationshipPath(graph, focus.id, compareTarget.id)
  }, [compareTarget, focus, graph])

  const pathNodes = React.useMemo(
    () => new Set(shortestPath),
    [shortestPath]
  )
  const pathEdges = React.useMemo(
    () => pathEdgeIds(graph.edges, shortestPath),
    [graph.edges, shortestPath]
  )

  const focusId =
    focus?.kind === "company" || focus?.kind === "person" ? focus.id : null
  const preview = hoverEntity ?? relationshipEntity(focus)
  const previewNode = preview ? nodeById.get(preview.id) ?? null : null

  function recenterOnFocus() {
    const entity = relationshipEntity(focus)
    if (!entity) return
    setAnchorEntity(entity)
    setPopulationSelection(relationshipPopulation(selection))
    setHiddenNodeIds(new Set())
  }

  function togglePinned(entityId: string) {
    setPinnedNodeIds((current) => toggleSetValue(current, entityId))
  }

  function hideEntity(entityId: string) {
    setHiddenNodeIds((current) => {
      const next = new Set(current)
      next.add(entityId)
      return next
    })
  }

  function handoffEntity(surface: SurfaceId) {
    setActiveSurface(surface)
  }

  return (
    <main className="relative min-h-svh overflow-hidden bg-tipx-canvas text-tipx-ink">
      <div className="tipx-grid absolute inset-0 opacity-18" />
      <div className="tipx-vignette absolute inset-0" />

      <RelationshipControlDeck
        view={view}
        depth={depth}
        anchorEntity={anchorEntity}
        currentFocus={relationshipEntity(focus)}
        layoutRunning={layoutRunning}
        endpointConfigured={relationshipEndpointConfigured()}
        graphSearch={graphSearch}
        searchResults={searchResults}
        pathLength={shortestPath.length}
        onViewChange={setView}
        onDepthChange={setDepth}
        onGraphSearchChange={setGraphSearch}
        onFocusSearchResult={(node) => {
          const entity = relationshipNodeToEntity(node)
          setFocus(entity)
          setGraphSearch("")
        }}
        onRecenter={recenterOnFocus}
        onClearFocus={() => setFocus(null)}
        onHandoff={handoffEntity}
      />

      {view === "explore" ? (
        graphQuery.isError ? (
          <RelationshipState
            icon={AlertTriangle}
            title="Relationship graph request failed"
            detail={errorMessage(graphQuery.error)}
          />
        ) : (
          <>
            <RelationshipNetwork
              snapshot={graph}
              focusId={focusId}
              hiddenNodeIds={hiddenNodeIds}
              pinnedNodeIds={pinnedNodeIds}
              pathNodeIds={pathNodes}
              pathEdgeIds={pathEdges}
              onFocus={setFocus}
              onToggleSelection={toggleSelection}
              onHover={setHoverEntity}
              onLayoutStateChange={setLayoutRunning}
            />
            <GraphAvailability snapshot={graph} pending={graphQuery.isPending} />
          </>
        )
      ) : null}

      {view === "neighbourhood" ? (
        graphQuery.isError ? (
          <RelationshipState
            icon={AlertTriangle}
            title="Neighbourhood request failed"
            detail={errorMessage(graphQuery.error)}
          />
        ) : (
          <>
            <RelationshipNeighbourhood
              snapshot={graph}
              focusId={focusId}
              hiddenNodeIds={hiddenNodeIds}
              pathNodeIds={pathNodes}
              pathEdgeIds={pathEdges}
              onFocus={setFocus}
              onToggleSelection={toggleSelection}
              onHover={setHoverEntity}
              onLayoutStateChange={setLayoutRunning}
            />
            <GraphAvailability snapshot={graph} pending={graphQuery.isPending} />
          </>
        )
      ) : null}

      {view === "matrix" ? (
        <BoardMatrix
          snapshot={matrix}
          synchronized={matrixSynchronized}
          loading={matrixQuery.isPending || matrixQuery.isFetching}
          errorMessage={
            matrixQuery.isError ? errorMessage(matrixQuery.error) : null
          }
          companyQuery={matrixCompanyQuery}
          personQuery={matrixPersonQuery}
          onCompanyQueryChange={setMatrixCompanyQuery}
          onPersonQueryChange={setMatrixPersonQuery}
          onFocusCompany={(entity) => {
            setFocus(entity)
            setActiveSurface("C")
          }}
          onFocusPerson={(entity) => {
            setFocus(entity)
            setActiveSurface("P")
          }}
          onSetSelection={setSelection}
          onOpenNetwork={() => setView("explore")}
        />
      ) : null}

      {view === "genealogy" ? (
        focus?.kind !== "company" ? (
          <RelationshipState
            icon={GitFork}
            title="Genealogy needs a company focus"
            detail="Focus a company from Map, Company, People, Matrix or the relationship graph before switching to directed genealogy."
          />
        ) : genealogyQuery.isPending ? (
          <RelationshipState
            icon={LoaderCircle}
            title="Loading directed genealogy"
            spinning
          />
        ) : genealogyQuery.isError ? (
          <RelationshipState
            icon={AlertTriangle}
            title="Genealogy request failed"
            detail={errorMessage(genealogyQuery.error)}
          />
        ) : (
          <RelationshipGenealogy
            snapshot={genealogy}
            onFocusCompany={(entity) => setFocus(entity)}
          />
        )
      ) : null}

      {(view === "explore" || view === "neighbourhood") && preview ? (
        <RelationshipInspector
          entity={preview}
          node={previewNode}
          pinned={pinnedNodeIds.has(preview.id)}
          selected={selection.some(
            (entity) =>
              entity.kind === preview.kind && entity.id === preview.id
          )}
          compared={compare.some(
            (entity) =>
              entity.kind === preview.kind && entity.id === preview.id
          )}
          onFocus={() => setFocus(preview)}
          onPin={() => togglePinned(preview.id)}
          onHide={() => hideEntity(preview.id)}
          onSelection={() => toggleSelection(preview)}
          onCompare={() => toggleCompare(preview)}
          onOpenEntity={() =>
            setActiveSurface(preview.kind === "person" ? "P" : "C")
          }
        />
      ) : null}
    </main>
  )
}

function RelationshipControlDeck({
  view,
  depth,
  anchorEntity,
  currentFocus,
  layoutRunning,
  endpointConfigured,
  graphSearch,
  searchResults,
  pathLength,
  onViewChange,
  onDepthChange,
  onGraphSearchChange,
  onFocusSearchResult,
  onRecenter,
  onClearFocus,
  onHandoff,
}: {
  view: RelationshipViewId
  depth: number
  anchorEntity: EntityRef | null
  currentFocus: EntityRef | null
  layoutRunning: boolean
  endpointConfigured: boolean
  graphSearch: string
  searchResults: RelationshipNode[]
  pathLength: number
  onViewChange: (view: RelationshipViewId) => void
  onDepthChange: (depth: number) => void
  onGraphSearchChange: (value: string) => void
  onFocusSearchResult: (node: RelationshipNode) => void
  onRecenter: () => void
  onClearFocus: () => void
  onHandoff: (surface: SurfaceId) => void
}) {
  const graphMode = view === "explore" || view === "neighbourhood"

  return (
    <aside className="pointer-events-auto fixed left-4 right-[4.75rem] top-4 z-[45] sm:left-7 sm:right-auto sm:top-7 sm:w-[390px]">
      <div className="rounded-2xl border border-tipx-line/10 bg-tipx-floating/91 p-2 shadow-[0_24px_80px_rgba(0,0,0,.38)] backdrop-blur-2xl">
        <div className="flex items-center gap-2">
          <span className="grid size-9 shrink-0 place-items-center rounded-xl bg-tipx-brand font-mono text-[11px] font-bold text-tipx-on-brand">
            R
          </span>
          <div className="min-w-0 flex-1">
            <p className="truncate text-[10px] font-semibold text-tipx-ink/58">
              Relationship Observatory
            </p>
            <p className="mt-0.5 truncate text-[8px] uppercase tracking-[0.12em] text-tipx-ink/21">
              {anchorEntity
                ? "anchor · " + anchorEntity.label
                : "Lens / Selection population"}
            </p>
          </div>
          {currentFocus ? (
            <button
              type="button"
              onClick={onClearFocus}
              className="grid size-7 shrink-0 place-items-center rounded-lg border border-tipx-line/7 text-tipx-ink/24 transition hover:bg-tipx-ink/[0.055] hover:text-tipx-ink"
              aria-label={"Clear focused " + currentFocus.kind + ": " + currentFocus.label}
              title="Clear current Focus"
            >
              <X className="size-3" />
            </button>
          ) : null}
          <span
            className={cn(
              "size-1.5 rounded-full",
              endpointConfigured ? "bg-emerald-300/70" : "bg-tipx-ink/16"
            )}
          />
        </div>

        <div className="mt-2 grid grid-cols-4 gap-1">
          {VIEW_META.map((meta) => {
            const Icon = meta.icon
            const active = view === meta.id
            return (
              <button
                key={meta.id}
                type="button"
                onClick={() => onViewChange(meta.id)}
                className={cn(
                  "group rounded-xl border px-2 py-2 text-left transition",
                  active
                    ? "border-tipx-line/20 bg-tipx-brand text-tipx-on-brand"
                    : "border-tipx-line/7 bg-tipx-ink/[0.018] text-tipx-ink/34 hover:bg-tipx-ink/[0.055] hover:text-tipx-ink"
                )}
                title={meta.label}
              >
                <span className="flex items-center gap-1.5">
                  <Icon className="size-3" />
                  <span className="font-mono text-[9px] font-semibold">
                    {meta.short}
                  </span>
                </span>
                <span className="mt-1 hidden text-[8px] sm:block">
                  {meta.label}
                </span>
              </button>
            )
          })}
        </div>

        {graphMode ? (
          <div className="mt-2 flex items-center gap-1.5">
            <div className="relative min-w-0 flex-1">
              <Search className="pointer-events-none absolute left-2.5 top-1/2 size-3 -translate-y-1/2 text-tipx-ink/22" />
              <input
                value={graphSearch}
                onChange={(event) => onGraphSearchChange(event.target.value)}
                placeholder="Search loaded graph…"
                data-keyboard-input
                className="h-9 w-full rounded-xl border border-tipx-line/7 bg-tipx-ink/[0.02] pl-8 pr-3 text-[9px] text-tipx-ink outline-none placeholder:text-tipx-ink/18 focus:border-tipx-line/14"
              />
              {graphSearch.trim() && searchResults.length > 0 ? (
                <div className="absolute left-0 right-0 top-[calc(100%+7px)] z-50 max-h-64 overflow-auto rounded-xl border border-tipx-line/9 bg-tipx-floating/98 p-1 shadow-2xl">
                  {searchResults.map((node) => (
                    <button
                      key={node.id}
                      type="button"
                      onClick={() => onFocusSearchResult(node)}
                      className="flex w-full items-center gap-2 rounded-lg px-2.5 py-2 text-left hover:bg-tipx-ink/[0.055]"
                    >
                      {node.kind === "person" ? (
                        <UserRound className="size-3 text-tipx-ink/30" />
                      ) : (
                        <Network className="size-3 text-tipx-ink/30" />
                      )}
                      <span className="min-w-0 flex-1 truncate text-[9px] text-tipx-ink/55">
                        {node.label}
                      </span>
                      <span className="font-mono text-[7px] text-tipx-ink/19">
                        d{node.degree}
                      </span>
                    </button>
                  ))}
                </div>
              ) : null}
            </div>

            <button
              type="button"
              onClick={onRecenter}
              className="grid size-9 place-items-center rounded-xl border border-tipx-line/7 bg-tipx-ink/[0.02] text-tipx-ink/34 hover:bg-tipx-ink/[0.06] hover:text-tipx-ink"
              title="Recenter graph population on current Focus"
            >
              <Anchor className="size-3.5" />
            </button>
          </div>
        ) : null}

        <div className="mt-2 flex items-center gap-2 border-t border-tipx-line/7 pt-2">
          <span className="text-[8px] uppercase tracking-[0.12em] text-tipx-ink/20">
            Depth
          </span>
          {[1, 2, 3, 4].map((value) => (
            <button
              key={value}
              type="button"
              onClick={() => onDepthChange(value)}
              className={cn(
                "grid size-7 place-items-center rounded-lg border font-mono text-[8px] transition",
                depth === value
                  ? "border-tipx-line/18 bg-tipx-brand text-tipx-on-brand"
                  : "border-tipx-line/7 bg-tipx-ink/[0.018] text-tipx-ink/30 hover:bg-tipx-ink/[0.05]"
              )}
            >
              {value}
            </button>
          ))}
          {layoutRunning ? (
            <span className="ml-1 inline-flex items-center gap-1.5 text-[8px] uppercase tracking-[0.1em] text-tipx-ink/25">
              <LoaderCircle className="size-3 animate-spin" />
              Layout
            </span>
          ) : null}
          {pathLength > 1 ? (
            <span className="ml-auto inline-flex items-center gap-1.5 rounded-full border border-emerald-200/10 bg-emerald-200/[0.035] px-2 py-1 font-mono text-[7px] text-emerald-100/55">
              path {pathLength - 1}
            </span>
          ) : null}
          <button
            type="button"
            onClick={() => onHandoff("M")}
            className="ml-auto grid size-7 place-items-center rounded-lg border border-tipx-line/7 text-tipx-ink/27 hover:bg-tipx-ink/[0.05] hover:text-tipx-ink"
            title="Project selected companies to Map"
          >
            <MapIcon className="size-3" />
          </button>
        </div>
      </div>
    </aside>
  )
}

function RelationshipInspector({
  entity,
  node,
  pinned,
  selected,
  compared,
  onFocus,
  onPin,
  onHide,
  onSelection,
  onCompare,
  onOpenEntity,
}: {
  entity: EntityRef
  node: RelationshipNode | null
  pinned: boolean
  selected: boolean
  compared: boolean
  onFocus: () => void
  onPin: () => void
  onHide: () => void
  onSelection: () => void
  onCompare: () => void
  onOpenEntity: () => void
}) {
  return (
    <aside className="pointer-events-auto fixed bottom-20 right-5 z-[42] w-[min(340px,calc(100vw-40px))] rounded-2xl border border-tipx-line/10 bg-tipx-floating/91 p-3 shadow-[0_24px_80px_rgba(0,0,0,.36)] backdrop-blur-2xl sm:right-7">
      <div className="flex items-start gap-3">
        <span className="grid size-10 shrink-0 place-items-center rounded-xl border border-tipx-line/8 bg-tipx-ink/[0.03]">
          {entity.kind === "person" ? (
            <UserRound className="size-4 text-tipx-ink/40" />
          ) : (
            <Network className="size-4 text-tipx-ink/40" />
          )}
        </span>
        <div className="min-w-0 flex-1">
          <p className="truncate text-[11px] font-semibold text-tipx-ink/67">
            {entity.label}
          </p>
          <p className="mt-1 truncate text-[8px] text-tipx-ink/23">
            {entity.secondary || entity.kind}
          </p>
        </div>
        {node ? (
          <span className="font-mono text-[8px] text-tipx-ink/22">
            d{node.degree}
          </span>
        ) : null}
      </div>

      <div className="mt-3 grid grid-cols-3 gap-1.5">
        <InspectorAction icon={Focus} label="Focus" active={false} onClick={onFocus} />
        <InspectorAction icon={Pin} label="Pin" active={pinned} onClick={onPin} />
        <InspectorAction icon={EyeOff} label="Hide" active={false} onClick={onHide} />
        <InspectorAction
          icon={Network}
          label="Select"
          active={selected}
          onClick={onSelection}
        />
        <InspectorAction
          icon={GitCompare}
          label="Compare"
          active={compared}
          onClick={onCompare}
        />
        <InspectorAction
          icon={entity.kind === "person" ? UserRound : Network}
          label={entity.kind === "person" ? "Person" : "Company"}
          active={false}
          onClick={onOpenEntity}
        />
      </div>
    </aside>
  )
}

function InspectorAction({
  icon: Icon,
  label,
  active,
  onClick,
}: {
  icon: typeof Network
  label: string
  active: boolean
  onClick: () => void
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={cn(
        "flex items-center justify-center gap-1.5 rounded-xl border px-2 py-2 text-[8px] transition",
        active
          ? "border-tipx-line/18 bg-tipx-brand text-tipx-on-brand"
          : "border-tipx-line/7 bg-tipx-ink/[0.018] text-tipx-ink/31 hover:bg-tipx-ink/[0.06] hover:text-tipx-ink"
      )}
    >
      <Icon className="size-3" />
      {label}
    </button>
  )
}

function GraphAvailability({
  snapshot,
  pending,
}: {
  snapshot: RelationshipGraphSnapshot
  pending: boolean
}) {
  if (pending) {
    return (
      <RelationshipState
        icon={LoaderCircle}
        title="Loading relationship population"
        spinning
      />
    )
  }

  if (snapshot.availability === "unavailable" || snapshot.nodes.length === 0) {
    return (
      <RelationshipState
        icon={Database}
        title="Relationship capability unavailable"
        detail={
          snapshot.message ||
          "No production relationship population is available for this context."
        }
      />
    )
  }

  return null
}

function RelationshipState({
  icon: Icon,
  title,
  detail,
  spinning = false,
}: {
  icon: typeof Network
  title: string
  detail?: string
  spinning?: boolean
}) {
  return (
    <div className="pointer-events-none absolute inset-0 grid place-items-center px-8 text-center">
      <div className="rounded-2xl border border-tipx-line/8 bg-tipx-floating/76 px-6 py-5 backdrop-blur-xl">
        <Icon
          className={
            "mx-auto size-5 text-tipx-ink/24 " + (spinning ? "animate-spin" : "")
          }
        />
        <p className="mt-3 text-[11px] font-medium text-tipx-ink/48">{title}</p>
        {detail ? (
          <p className="mx-auto mt-1 max-w-md text-[9px] leading-4 text-tipx-ink/25">
            {detail}
          </p>
        ) : null}
      </div>
    </div>
  )
}

function EngineLoader({ label }: { label: string }) {
  return (
    <div className="absolute inset-0 grid place-items-center">
      <div className="flex items-center gap-2 rounded-full border border-tipx-line/7 bg-tipx-floating/72 px-3 py-2 text-[8px] uppercase tracking-[0.13em] text-tipx-ink/25 backdrop-blur-xl">
        <LoaderCircle className="size-3 animate-spin" />
        {label}
      </div>
    </div>
  )
}

function relationshipEntity(entity: EntityRef | null | undefined) {
  if (!entity) return null
  return entity.kind === "company" || entity.kind === "person" ? entity : null
}

function relationshipPopulation(entities: EntityRef[]) {
  return entities.filter(
    (entity) => entity.kind === "company" || entity.kind === "person"
  )
}

function toggleSetValue(current: Set<string>, value: string) {
  const next = new Set(current)
  if (next.has(value)) next.delete(value)
  else next.add(value)
  return next
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : "Relationship serving failed."
}
