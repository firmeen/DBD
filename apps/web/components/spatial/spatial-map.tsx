"use client"

import * as React from "react"
import type { Layer, MapViewState } from "@deck.gl/core"
import { H3HexagonLayer } from "@deck.gl/geo-layers"
import {
  GeoJsonLayer,
  IconLayer,
  ScatterplotLayer,
  TextLayer,
} from "@deck.gl/layers"
import { DeckGL } from "@deck.gl/react"
import { circle } from "@turf/turf"
import { setWorkerUrl } from "maplibre-gl"
import { Map } from "react-map-gl/maplibre"
import Supercluster from "supercluster"

import { aggregateCompanyPointsToH3 } from "@/lib/spatial/h3"
import { spatialLodForZoom } from "@/lib/spatial/lod"
import type {
  SpatialBranch,
  SpatialCompanyPoint,
  SpatialLayerId,
  SpatialSnapshot,
  SpatialViewState,
} from "@/lib/spatial/types"

setWorkerUrl("/maplibre/maplibre-gl-worker.mjs")

const DEFAULT_MAP_STYLE =
  process.env.NEXT_PUBLIC_TIPX_MAP_STYLE_URL?.trim() ||
  "https://tiles.openfreemap.org/styles/liberty"

type HoveredCompany = {
  company: SpatialCompanyPoint
  x: number
  y: number
}

type ClusterGlyph = {
  id: number
  coordinates: [number, number]
  count: number
}

type CompanyFeatureProperties = {
  company: SpatialCompanyPoint
}

type ClusterFeatureProperties = {
  cluster?: boolean
  cluster_id?: number
  point_count?: number
  company?: SpatialCompanyPoint
}

export function SpatialMap({
  snapshot,
  viewState,
  layers: enabledLayers,
  branchId,
  radiusKm,
  selectedCompanyIds,
  hoveredCompanyId,
  onViewStateChange,
  onHoverCompany,
  onFocusCompany,
  onFocusBranch,
}: {
  snapshot: SpatialSnapshot
  viewState: SpatialViewState
  layers: SpatialLayerId[]
  branchId: string | null
  radiusKm: number
  selectedCompanyIds: readonly string[]
  hoveredCompanyId: string | null
  onViewStateChange: (viewState: SpatialViewState) => void
  onHoverCompany: (hover: HoveredCompany | null) => void
  onFocusCompany: (company: SpatialCompanyPoint) => void
  onFocusBranch: (branch: SpatialBranch) => void
}) {
  const selectedIds = React.useMemo(
    () => new Set(selectedCompanyIds),
    [selectedCompanyIds]
  )
  const lod = spatialLodForZoom(viewState.zoom)

  const h3Cells = React.useMemo(
    () =>
      snapshot.h3Cells.length > 0
        ? snapshot.h3Cells
        : aggregateCompanyPointsToH3(
            snapshot.companies,
            lod === "national" ? 4 : lod === "regional" ? 6 : 8
          ),
    [lod, snapshot.companies, snapshot.h3Cells]
  )

  const clusterIndex = React.useMemo(() => {
    const index = new Supercluster<
      CompanyFeatureProperties,
      Record<string, never>
    >({
      radius: 54,
      maxZoom: 14,
      minPoints: 3,
    })

    index.load(
      snapshot.companies.map((company) => ({
        type: "Feature" as const,
        geometry: {
          type: "Point" as const,
          coordinates: company.coordinates,
        },
        properties: { company },
      }))
    )

    return index
  }, [snapshot.companies])

  const clustered = React.useMemo(() => {
    if (lod !== "local") {
      return {
        clusters: [] as ClusterGlyph[],
        companies: [] as SpatialCompanyPoint[],
      }
    }

    const clusters: ClusterGlyph[] = []
    const companies: SpatialCompanyPoint[] = []

    for (const feature of clusterIndex.getClusters(
      [-180, -85, 180, 85],
      Math.floor(viewState.zoom)
    )) {
      const properties = feature.properties as ClusterFeatureProperties
      const [longitude, latitude] = feature.geometry.coordinates

      if (
        properties.cluster &&
        typeof properties.cluster_id === "number" &&
        typeof properties.point_count === "number"
      ) {
        clusters.push({
          id: properties.cluster_id,
          coordinates: [longitude, latitude],
          count: properties.point_count,
        })
      } else if (properties.company) {
        companies.push(properties.company)
      }
    }

    return { clusters, companies }
  }, [clusterIndex, lod, viewState.zoom])

  const branch = React.useMemo(
    () => snapshot.branches.find((candidate) => candidate.id === branchId) ?? null,
    [branchId, snapshot.branches]
  )

  const catchment = React.useMemo(
    () =>
      branch
        ? circle(branch.coordinates, radiusKm, {
            steps: 96,
            units: "kilometers",
          })
        : null,
    [branch, radiusKm]
  )

  const deckLayers = React.useMemo(() => {
    const result: Layer[] = []
    const enabled = new Set(enabledLayers)

    if (
      enabled.has("administrative") &&
      snapshot.capabilities.administrativeGeometry &&
      snapshot.areas.length > 0
    ) {
      result.push(
        new GeoJsonLayer({
          id: "tipx-administrative-areas",
          data: {
            type: "FeatureCollection",
            features: snapshot.areas.map((area) => ({
              type: "Feature",
              geometry: area.geometry,
              properties: {
                id: area.id,
                name: area.name,
                level: area.level,
                companyCount: area.companyCount,
                tipPenetration: area.tipPenetration,
              },
            })),
          },
          filled: true,
          stroked: true,
          getFillColor: [72, 96, 118, 34],
          getLineColor: [210, 226, 238, 62],
          getLineWidth: 1,
          lineWidthUnits: "pixels",
          pickable: false,
        })
      )
    }

    if (
      enabled.has("density") &&
      (lod === "national" || lod === "regional") &&
      h3Cells.length > 0
    ) {
      result.push(
        new H3HexagonLayer({
          id: "tipx-h3-density",
          data: h3Cells,
          getHexagon: (cell) => cell.index,
          getFillColor: (cell) => {
            const intensity = Math.min(188, 54 + Math.log2(cell.count + 1) * 24)
            return [33, 175, 210, Math.round(intensity)]
          },
          getLineColor: [12, 20, 28, 120],
          getLineWidth: 1,
          lineWidthUnits: "pixels",
          extruded: false,
          pickable: true,
        })
      )
    }

    if (
      enabled.has("opportunity") &&
      snapshot.capabilities.opportunityOverlay &&
      h3Cells.some((cell) => cell.opportunityScore !== null && cell.opportunityScore !== undefined)
    ) {
      result.push(
        new H3HexagonLayer({
          id: "tipx-h3-opportunity",
          data: h3Cells.filter(
            (cell) =>
              cell.opportunityScore !== null &&
              cell.opportunityScore !== undefined
          ),
          getHexagon: (cell) => cell.index,
          getFillColor: (cell) => {
            const score = Math.max(0, Math.min(1, cell.opportunityScore ?? 0))
            return [
              Math.round(120 + 100 * score),
              Math.round(130 + 70 * score),
              54,
              Math.round(44 + 146 * score),
            ]
          },
          getLineColor: [20, 20, 14, 120],
          getLineWidth: 1,
          lineWidthUnits: "pixels",
          pickable: true,
        })
      )
    }

    if (
      enabled.has("catchment") &&
      branch &&
      catchment &&
      snapshot.capabilities.branches
    ) {
      result.push(
        new GeoJsonLayer({
          id: "tipx-branch-catchment",
          data: catchment,
          filled: true,
          stroked: true,
          getFillColor: [105, 211, 176, 28],
          getLineColor: [139, 232, 199, 180],
          getLineWidth: 2,
          lineWidthUnits: "pixels",
          pickable: false,
        })
      )
    }

    if (
      enabled.has("selected") &&
      selectedIds.size > 0 &&
      snapshot.companies.length > 0
    ) {
      const selectedCompanies = snapshot.companies.filter((company) =>
        selectedIds.has(company.id)
      )

      result.push(
        new ScatterplotLayer({
          id: "tipx-selected-companies",
          data: selectedCompanies,
          getPosition: (company) => company.coordinates,
          getRadius: 18,
          radiusUnits: "pixels",
          stroked: true,
          filled: false,
          getLineColor: [142, 238, 205, 230],
          getLineWidth: 3,
          lineWidthUnits: "pixels",
          pickable: false,
        })
      )
    }

    if (
      enabled.has("companies") &&
      snapshot.capabilities.preciseCompanyGeometry
    ) {
      if (lod === "local") {
        if (clustered.clusters.length > 0) {
          result.push(
            new ScatterplotLayer({
              id: "tipx-company-clusters",
              data: clustered.clusters,
              getPosition: (cluster) => cluster.coordinates,
              getRadius: (cluster) =>
                Math.min(28, 12 + Math.log2(cluster.count + 1) * 3),
              radiusUnits: "pixels",
              getFillColor: [18, 24, 30, 218],
              getLineColor: [210, 227, 238, 150],
              stroked: true,
              getLineWidth: 1,
              lineWidthUnits: "pixels",
              pickable: true,
              onClick: ({ object }) => {
                const cluster = object as ClusterGlyph | undefined
                if (!cluster) return

                const nextZoom = clusterIndex.getClusterExpansionZoom(cluster.id)
                onViewStateChange({
                  ...viewState,
                  longitude: cluster.coordinates[0],
                  latitude: cluster.coordinates[1],
                  zoom: Math.min(16, Math.max(viewState.zoom + 1, nextZoom)),
                })
              },
            })
          )

          result.push(
            new TextLayer({
              id: "tipx-company-cluster-counts",
              data: clustered.clusters,
              getPosition: (cluster) => cluster.coordinates,
              getText: (cluster) => compactNumber(cluster.count),
              getSize: 10,
              sizeUnits: "pixels",
              getColor: [245, 248, 250, 220],
              getTextAnchor: "middle",
              getAlignmentBaseline: "center",
              pickable: false,
            })
          )
        }

        if (clustered.companies.length > 0) {
          result.push(
            createCompanyEntityLayer(
              "tipx-local-companies",
              clustered.companies,
              selectedIds,
              hoveredCompanyId,
              onHoverCompany,
              onFocusCompany
            )
          )
        }
      }

      if (lod === "entity") {
        result.push(
          createCompanyEntityLayer(
            "tipx-company-entities",
            snapshot.companies,
            selectedIds,
            hoveredCompanyId,
            onHoverCompany,
            onFocusCompany
          )
        )
      }
    }

    if (
      enabled.has("branches") &&
      snapshot.capabilities.branches &&
      snapshot.branches.length > 0
    ) {
      result.push(
        new ScatterplotLayer({
          id: "tipx-branches",
          data: snapshot.branches,
          getPosition: (item) => item.coordinates,
          getRadius: (item) => (item.id === branchId ? 10 : 7),
          radiusUnits: "pixels",
          getFillColor: (item) =>
            item.id === branchId
              ? [240, 246, 250, 245]
              : [169, 185, 197, 210],
          getLineColor: [7, 10, 13, 220],
          stroked: true,
          getLineWidth: 2,
          lineWidthUnits: "pixels",
          pickable: true,
          onClick: ({ object }) => {
            const item = object as SpatialBranch | undefined
            if (item) onFocusBranch(item)
          },
        })
      )

      result.push(
        new TextLayer({
          id: "tipx-branch-labels",
          data: snapshot.branches,
          getPosition: (item) => item.coordinates,
          getText: (item) => item.name,
          getPixelOffset: [0, -18],
          getSize: 10,
          sizeUnits: "pixels",
          getColor: [245, 248, 250, 190],
          getTextAnchor: "middle",
          getAlignmentBaseline: "bottom",
          background: true,
          getBackgroundColor: [7, 10, 13, 185],
          backgroundPadding: [5, 3],
          pickable: false,
        })
      )
    }

    return result
  }, [
    branch,
    branchId,
    catchment,
    clustered.clusters,
    clustered.companies,
    clusterIndex,
    enabledLayers,
    h3Cells,
    hoveredCompanyId,
    lod,
    onFocusBranch,
    onFocusCompany,
    onHoverCompany,
    onViewStateChange,
    selectedIds,
    snapshot,
    viewState,
  ])

  return (
    <DeckGL
      viewState={viewState as MapViewState}
      controller={{
        dragPan: true,
        dragRotate: true,
        scrollZoom: true,
        touchZoom: true,
        touchRotate: true,
        doubleClickZoom: true,
        keyboard: true,
      }}
      layers={deckLayers}
      onViewStateChange={({ viewState: nextViewState }) =>
        onViewStateChange(nextViewState as SpatialViewState)
      }
      onHover={({ object }) => {
        if (!object) onHoverCompany(null)
      }}
      style={{ position: "absolute", inset: 0 }}
    >
      <Map
        mapStyle={DEFAULT_MAP_STYLE}
        attributionControl
        reuseMaps
        style={{ width: "100%", height: "100%" }}
      />
    </DeckGL>
  )
}

function createCompanyEntityLayer(
  id: string,
  companies: SpatialCompanyPoint[],
  selectedIds: Set<string>,
  hoveredCompanyId: string | null,
  onHoverCompany: (hover: HoveredCompany | null) => void,
  onFocusCompany: (company: SpatialCompanyPoint) => void
) {
  return new IconLayer({
    id,
    data: companies,
    getPosition: (company) => company.coordinates,
    getIcon: (company) => ({
      url: companyIconDataUrl(
        company,
        selectedIds.has(company.id),
        company.id === hoveredCompanyId
      ),
      width: 64,
      height: 64,
      anchorX: 32,
      anchorY: 32,
    }),
    getSize: (company) =>
      selectedIds.has(company.id) || company.id === hoveredCompanyId ? 36 : 30,
    sizeUnits: "pixels",
    billboard: true,
    pickable: true,
    onHover: ({ object, x, y }) => {
      const company = object as SpatialCompanyPoint | undefined
      onHoverCompany(company ? { company, x, y } : null)
    },
    onClick: ({ object }) => {
      const company = object as SpatialCompanyPoint | undefined
      if (company) onFocusCompany(company)
    },
  })
}

function companyIconDataUrl(
  company: SpatialCompanyPoint,
  selected: boolean,
  hovered: boolean
) {
  const fill =
    company.tipStatus === "active"
      ? "#256d8a"
      : company.tipStatus === "expired"
        ? "#8a403f"
        : "#515b64"
  const border = selected ? "#8fe8c9" : hovered ? "#ffffff" : "#b9c4cc"
  const initial = escapeSvg(company.name.trim().charAt(0).toUpperCase() || "?")
  const svg =
    `<svg xmlns="http://www.w3.org/2000/svg" width="64" height="64" viewBox="0 0 64 64">` +
    `<rect x="5" y="5" width="54" height="54" rx="15" fill="${fill}" stroke="${border}" stroke-width="${selected ? 4 : 2}"/>` +
    `<text x="32" y="38" text-anchor="middle" font-family="Arial,sans-serif" font-size="23" font-weight="700" fill="#fff">${initial}</text>` +
    `</svg>`

  return `data:image/svg+xml;charset=utf-8,${encodeURIComponent(svg)}`
}

function escapeSvg(value: string) {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;")
}

function compactNumber(value: number) {
  return new Intl.NumberFormat("en", {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value)
}
