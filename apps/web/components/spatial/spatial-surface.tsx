"use client"

import * as React from "react"
import { useQuery } from "@tanstack/react-query"
import { AlertTriangle, Database, LoaderCircle } from "lucide-react"

import { SpatialCompanyRail } from "./spatial-company-rail"
import { SpatialControlDeck } from "./spatial-control-deck"
import { SpatialMap } from "./spatial-map"
import { useGlobalLens } from "@/hooks/use-global-lens"
import { fetchSpatialSnapshot } from "@/lib/spatial/adapter"
import { exactCompanyIdsForSpatial } from "@/lib/spatial/exact-selection"
import { canAggregateH3InBrowser } from "@/lib/spatial/h3"
import {
  h3ResolutionForZoom,
  spatialLodForZoom,
} from "@/lib/spatial/lod"
import {
  defaultSpatialSurfaceSnapshot,
  parseSpatialSurfaceSnapshot,
} from "@/lib/spatial/surface-snapshot"
import { spatialBoundsForView } from "@/lib/spatial/viewport"
import {
  companyToEntityRef,
  branchToEntityRef,
  type SpatialCompanyPoint,
  type SpatialLayerId,
  type SpatialSnapshot,
} from "@/lib/spatial/types"
import { useWorkspaceStore } from "@/stores/workspace-store"

type MapHover = {
  company: SpatialCompanyPoint
  x: number
  y: number
}

const PENDING_SNAPSHOT: SpatialSnapshot = {
  availability: "partial",
  message: "Loading spatial serving state…",
  generatedAt: null,
  capabilities: {
    administrativeGeometry: false,
    preciseCompanyGeometry: false,
    branches: false,
    h3Aggregates: false,
    opportunityOverlay: false,
  },
  totals: {
    companies: null,
    active: null,
    expired: null,
    prospect: null,
  },
  companies: [],
  branches: [],
  h3Cells: [],
  areas: [],
}

export function SpatialSurface() {
  const surfaceRef = React.useRef<HTMLElement>(null)
  const [surfaceSize, setSurfaceSize] = React.useState({
    width: 0,
    height: 0,
  })
  const { lens } = useGlobalLens()
  const snapshotPayload = useWorkspaceStore(
    (state) => state.surfaceSnapshots.M?.payload
  )
  const saveSurfaceSnapshot = useWorkspaceStore(
    (state) => state.saveSurfaceSnapshot
  )
  const selection = useWorkspaceStore((state) => state.selection)
  const population = useWorkspaceStore((state) => state.population)
  const focus = useWorkspaceStore((state) => state.focus)
  const setFocus = useWorkspaceStore((state) => state.setFocus)
  const toggleSelection = useWorkspaceStore((state) => state.toggleSelection)
  const setActiveSurface = useWorkspaceStore((state) => state.setActiveSurface)

  const initialState = React.useMemo(
    () =>
      parseSpatialSurfaceSnapshot(snapshotPayload) ??
      defaultSpatialSurfaceSnapshot(),
    [snapshotPayload]
  )

  const [viewState, setViewState] = React.useState(initialState.viewState)
  const [queryViewState, setQueryViewState] = React.useState(
    initialState.viewState
  )
  const [layers, setLayers] = React.useState(initialState.layers)
  const [branchId, setBranchId] = React.useState(initialState.branchId)
  const [radiusKm, setRadiusKm] = React.useState(initialState.radiusKm)
  const [queryRadiusKm, setQueryRadiusKm] = React.useState(
    initialState.radiusKm
  )
  const [companyRailOpen, setCompanyRailOpen] = React.useState(
    initialState.companyRailOpen
  )
  const [mapHover, setMapHover] = React.useState<MapHover | null>(null)
  const [railHoverId, setRailHoverId] = React.useState<string | null>(null)
  const liveSnapshotRef = React.useRef(initialState)

  React.useLayoutEffect(() => {
    liveSnapshotRef.current = {
      viewState,
      layers,
      branchId,
      radiusKm,
      companyRailOpen,
    }
  }, [branchId, companyRailOpen, layers, radiusKm, viewState])

  React.useEffect(
    () => () => {
      const latest = liveSnapshotRef.current
      saveSurfaceSnapshot("M", {
        viewState: latest.viewState,
        layers: latest.layers,
        branchId: latest.branchId,
        radiusKm: latest.radiusKm,
        companyRailOpen: latest.companyRailOpen,
      })
    },
    [saveSurfaceSnapshot]
  )

  React.useEffect(() => {
    const node = surfaceRef.current
    if (!node) return

    const updateSize = () => {
      const rect = node.getBoundingClientRect()
      setSurfaceSize((current) =>
        current.width === rect.width && current.height === rect.height
          ? current
          : { width: rect.width, height: rect.height }
      )
    }

    updateSize()
    const observer = new ResizeObserver(updateSize)
    observer.observe(node)

    return () => observer.disconnect()
  }, [])

  React.useEffect(() => {
    const timeout = window.setTimeout(() => {
      setQueryViewState(viewState)
    }, 220)

    return () => window.clearTimeout(timeout)
  }, [viewState])

  React.useEffect(() => {
    const timeout = window.setTimeout(() => {
      setQueryRadiusKm(radiusKm)
    }, 420)

    return () => window.clearTimeout(timeout)
  }, [radiusKm])

  React.useEffect(() => {
    saveSurfaceSnapshot("M", {
      viewState: queryViewState,
      layers,
      branchId,
      radiusKm,
      companyRailOpen,
    })
  }, [
    branchId,
    companyRailOpen,
    layers,
    queryViewState,
    radiusKm,
    saveSurfaceSnapshot,
  ])

  const exactCompanyIds = React.useMemo(
    () => exactCompanyIdsForSpatial(selection),
    [selection]
  )

  const lod = spatialLodForZoom(queryViewState.zoom)
  const h3Resolution = h3ResolutionForZoom(queryViewState.zoom)
  const bounds = React.useMemo(
    () =>
      spatialBoundsForView(
        queryViewState,
        surfaceSize.width,
        surfaceSize.height
      ),
    [queryViewState, surfaceSize.height, surfaceSize.width]
  )

  const spatialQuery = React.useMemo(
    () => ({
      viewport: queryViewState,
      bounds,
      lod,
      h3Resolution,
      lens,
      population,
      exactCompanyIds,
      layers,
      branchId,
      radiusKm: queryRadiusKm,
    }),
    [
      bounds,
      branchId,
      h3Resolution,
      exactCompanyIds,
      layers,
      lens,
      lod,
      population,
      queryViewState,
      queryRadiusKm,
    ]
  )

  const query = useQuery({
    queryKey: ["tipx", "spatial", spatialQuery],
    queryFn: ({ signal }) => fetchSpatialSnapshot(spatialQuery, signal),
    staleTime: 30_000,
    placeholderData: (previous) => previous,
    retry: 1,
  })

  const snapshot = query.data ?? PENDING_SNAPSHOT
  const selectedCompanyIds = React.useMemo(
    () =>
      selection
        .filter((entity) => entity.kind === "company")
        .map((entity) => entity.id),
    [selection]
  )
  const selectedCompanySet = React.useMemo(
    () => new Set(selectedCompanyIds),
    [selectedCompanyIds]
  )

  const hoveredCompanyId = mapHover?.company.id ?? railHoverId

  function toggleLayer(layer: SpatialLayerId) {
    setLayers((current) =>
      current.includes(layer)
        ? current.filter((candidate) => candidate !== layer)
        : [...current, layer]
    )
  }

  function focusCompany(company: SpatialCompanyPoint) {
    setFocus(companyToEntityRef(company))
  }

  function focusBranch(id: string) {
    const branch = snapshot.branches.find((item) => item.id === id)
    if (branch) setFocus(branchToEntityRef(branch))
  }

  return (
    <main
      ref={surfaceRef}
      className="relative min-h-svh overflow-hidden bg-tipx-canvas text-tipx-ink"
    >
      <div className="absolute inset-0">
        <SpatialMap
          snapshot={snapshot}
          viewState={viewState}
          layers={layers}
          branchId={branchId}
          radiusKm={radiusKm}
          selectedCompanyIds={selectedCompanyIds}
          hoveredCompanyId={hoveredCompanyId}
          onViewStateChange={setViewState}
          onHoverCompany={setMapHover}
          onFocusCompany={focusCompany}
          onFocusBranch={(branch) => {
            setBranchId(branch.id)
            setFocus(branchToEntityRef(branch))
          }}
        />
      </div>

      <div className="pointer-events-none absolute inset-0 bg-[linear-gradient(to_bottom,rgba(7,10,13,.20),transparent_18%,transparent_76%,rgba(7,10,13,.42))]" />

      <SpatialControlDeck
        lod={lod}
        zoom={queryViewState.zoom}
        availability={snapshot.availability}
        message={
          query.error instanceof Error
            ? query.error.message
            : snapshot.message
        }
        capabilities={snapshot.capabilities}
        totals={snapshot.totals}
        layers={layers}
        branches={snapshot.branches}
        branchId={branchId}
        radiusKm={radiusKm}
        isFetching={query.isFetching}
        densityFallbackAvailable={
          snapshot.capabilities.preciseCompanyGeometry &&
          canAggregateH3InBrowser(snapshot.companies.length)
        }
        onToggleLayer={toggleLayer}
        onBranchChange={(nextBranchId) => {
          setBranchId(nextBranchId)
          if (nextBranchId) focusBranch(nextBranchId)
        }}
        onRadiusChange={setRadiusKm}
        onHandoff={setActiveSurface}
      />

      {focus ? null : (
        <SpatialCompanyRail
          companies={snapshot.companies}
          open={companyRailOpen}
          hoveredCompanyId={hoveredCompanyId}
          selectedCompanyIds={selectedCompanySet}
          onOpenChange={setCompanyRailOpen}
          onHoverCompany={(company) => setRailHoverId(company?.id ?? null)}
          onFocusCompany={focusCompany}
          onToggleSelection={(company) =>
            toggleSelection(companyToEntityRef(company))
          }
        />
      )}

      {mapHover ? (
        <div
          className="pointer-events-none absolute z-[42] w-60 rounded-xl border border-tipx-line/12 bg-tipx-floating/94 px-3 py-2.5 shadow-2xl backdrop-blur-xl"
          style={{
            left: Math.min(mapHover.x + 14, window.innerWidth - 260),
            top: Math.min(mapHover.y + 14, window.innerHeight - 150),
          }}
        >
          <p className="truncate text-[11px] font-semibold text-tipx-ink/82">
            {mapHover.company.name}
          </p>
          <p className="mt-1 truncate text-[9px] text-tipx-ink/35">
            {[
              mapHover.company.businessType,
              mapHover.company.district,
              mapHover.company.province,
            ]
              .filter(Boolean)
              .join(" · ") || "Company entity"}
          </p>
          <div className="mt-2 flex items-center gap-2 text-[9px] uppercase tracking-[0.12em] text-tipx-ink/28">
            <span>{mapHover.company.tipStatus ?? "TIP status unknown"}</span>
            {mapHover.company.boardCount !== null &&
            mapHover.company.boardCount !== undefined ? (
              <span>· {mapHover.company.boardCount} board</span>
            ) : null}
          </div>
        </div>
      ) : null}

      {query.isPending ? (
        <SpatialStatePill
          icon={LoaderCircle}
          label="Loading spatial contract"
          spinning
        />
      ) : query.isError ? (
        <SpatialStatePill
          icon={AlertTriangle}
          label="Spatial serving request unavailable"
        />
      ) : snapshot.availability === "unavailable" ? (
        <SpatialStatePill
          icon={Database}
          label="Basemap live · analytical geometry unavailable"
        />
      ) : null}
    </main>
  )
}

function SpatialStatePill({
  icon: Icon,
  label,
  spinning = false,
}: {
  icon: typeof Database
  label: string
  spinning?: boolean
}) {
  return (
    <div className="pointer-events-none fixed left-1/2 top-6 z-[39] -translate-x-1/2 rounded-full border border-tipx-line/9 bg-tipx-floating/82 px-3 py-2 text-[9px] uppercase tracking-[0.14em] text-tipx-ink/36 shadow-xl backdrop-blur-xl">
      <span className="flex items-center gap-2">
        <Icon className={`size-3 ${spinning ? "animate-spin" : ""}`} />
        {label}
      </span>
    </div>
  )
}
