"use client"

import * as React from "react"
import Graph from "graphology"
import forceAtlas2 from "graphology-layout-forceatlas2"
import Sigma from "sigma"

import { useTipxTheme } from "@/hooks/use-tipx-theme"
import type { TipxRenderPalette } from "@/lib/theme/palette"
import type {
  RelationshipGraphSnapshot,
  RelationshipNode,
} from "@/lib/relationship/types"
import { relationshipNodeToEntity } from "@/lib/relationship/types"
import type { EntityRef } from "@/lib/intelligence/types"

const MAX_CLIENT_FORCE_NODES = 600

export function RelationshipNetwork({
  snapshot,
  focusId,
  hiddenNodeIds,
  pinnedNodeIds,
  pathNodeIds,
  pathEdgeIds,
  onFocus,
  onToggleSelection,
  onHover,
  onLayoutStateChange,
}: {
  snapshot: RelationshipGraphSnapshot
  focusId: string | null
  hiddenNodeIds: ReadonlySet<string>
  pinnedNodeIds: ReadonlySet<string>
  pathNodeIds: ReadonlySet<string>
  pathEdgeIds: ReadonlySet<string>
  onFocus: (entity: EntityRef) => void
  onToggleSelection: (entity: EntityRef) => void
  onHover: (entity: EntityRef | null) => void
  onLayoutStateChange: (running: boolean) => void
}) {
  const { palette } = useTipxTheme()
  const paletteRef = React.useRef(palette)
  const containerRef = React.useRef<HTMLDivElement>(null)
  const rendererRef = React.useRef<Sigma | null>(null)
  const graphRef = React.useRef<Graph | null>(null)
  const hoveredRef = React.useRef<string | null>(null)
  const stateRef = React.useRef({
    focusId,
    hiddenNodeIds,
    pinnedNodeIds,
    pathNodeIds,
    pathEdgeIds,
  })

  React.useEffect(() => {
    paletteRef.current = palette

    const graph = graphRef.current
    if (graph) {
      const nodeById = new Map(snapshot.nodes.map((node) => [node.id, node]))
      const edgeById = new Map(snapshot.edges.map((edge) => [edge.id, edge]))

      graph.forEachNode((node) => {
        const source = nodeById.get(node)
        if (source) graph.setNodeAttribute(node, "color", nodeColor(source, palette))
      })

      graph.forEachEdge((edge) => {
        const source = edgeById.get(edge)
        if (source) graph.setEdgeAttribute(edge, "color", edgeColor(source.kind, palette))
      })
    }

    rendererRef.current?.refresh()
  }, [palette, snapshot.edges, snapshot.nodes])

  React.useEffect(() => {
    stateRef.current = {
      focusId,
      hiddenNodeIds,
      pinnedNodeIds,
      pathNodeIds,
      pathEdgeIds,
    }
    rendererRef.current?.refresh()
  }, [focusId, hiddenNodeIds, pathEdgeIds, pathNodeIds, pinnedNodeIds])

  React.useEffect(() => {
    const container = containerRef.current
    if (!container || snapshot.nodes.length === 0) return

    const activePalette = paletteRef.current
    const graph = buildGraph(snapshot, onLayoutStateChange, activePalette)
    graphRef.current = graph

    const nodeById = new Map(snapshot.nodes.map((node) => [node.id, node]))

    const renderer = new Sigma(graph, container, {
      renderEdgeLabels: false,
      hideEdgesOnMove: snapshot.nodes.length > 1_000,
      labelDensity: 0.08,
      labelGridCellSize: 90,
      labelRenderedSizeThreshold: 7,
      defaultNodeColor: activePalette.graph.node,
      defaultEdgeColor: activePalette.graph.edge,
      nodeReducer(node, data) {
        const currentPalette = paletteRef.current
        const current = stateRef.current
        const hidden = current.hiddenNodeIds.has(node)
        const hovered = hoveredRef.current
        const adjacentToHover =
          !hovered ||
          hovered === node ||
          graph.areNeighbors(hovered, node)

        return {
          ...data,
          hidden,
          color:
            hovered && !adjacentToHover
              ? currentPalette.graph.nodeMuted
              : current.pathNodeIds.has(node)
                ? currentPalette.graph.path
                : data.color,
          size:
            data.size *
            (current.focusId === node
              ? 1.65
              : current.pinnedNodeIds.has(node)
                ? 1.35
                : hovered === node
                  ? 1.25
                  : 1),
          forceLabel:
            current.focusId === node ||
            current.pinnedNodeIds.has(node) ||
            current.pathNodeIds.has(node) ||
            hovered === node,
          highlighted:
            current.focusId === node ||
            current.pinnedNodeIds.has(node) ||
            current.pathNodeIds.has(node),
        }
      },
      edgeReducer(edge, data) {
        const currentPalette = paletteRef.current
        const current = stateRef.current
        const [source, target] = graph.extremities(edge)
        const hovered = hoveredRef.current
        const hidden =
          current.hiddenNodeIds.has(source) ||
          current.hiddenNodeIds.has(target)
        const incidentToHover =
          !hovered || source === hovered || target === hovered

        return {
          ...data,
          hidden,
          color: current.pathEdgeIds.has(edge)
            ? currentPalette.graph.path
            : hovered && !incidentToHover
              ? currentPalette.graph.edgeMuted
              : data.color,
          size: current.pathEdgeIds.has(edge)
            ? Math.max(2.2, data.size)
            : incidentToHover
              ? data.size
              : Math.max(0.35, data.size * 0.65),
        }
      },
    })

    rendererRef.current = renderer

    renderer.on("enterNode", ({ node }) => {
      hoveredRef.current = node
      renderer.refresh()
      const relationshipNode = nodeById.get(node)
      onHover(
        relationshipNode ? relationshipNodeToEntity(relationshipNode) : null
      )
    })

    renderer.on("leaveNode", () => {
      hoveredRef.current = null
      renderer.refresh()
      onHover(null)
    })

    renderer.on("clickNode", ({ node }) => {
      const relationshipNode = nodeById.get(node)
      if (relationshipNode) {
        onFocus(relationshipNodeToEntity(relationshipNode))
      }
    })

    renderer.on("doubleClickNode", ({ node }) => {
      const relationshipNode = nodeById.get(node)
      if (relationshipNode) {
        onToggleSelection(relationshipNodeToEntity(relationshipNode))
      }
    })

    return () => {
      renderer.kill()
      rendererRef.current = null
      graphRef.current = null
      onHover(null)
    }
  }, [
    onFocus,
    onHover,
    onLayoutStateChange,
    onToggleSelection,
    snapshot,
  ])

  return (
    <div className="absolute inset-0">
      <div
        ref={containerRef}
        className="absolute inset-0"
        aria-label="Relationship network canvas"
      />
      <div className="pointer-events-none absolute bottom-5 left-5 rounded-full border border-tipx-line/7 bg-tipx-floating/76 px-3 py-1.5 text-[8px] uppercase tracking-[0.14em] text-tipx-ink/24 backdrop-blur-xl">
        Click focus · double click selection · wheel zoom · drag pan
      </div>
    </div>
  )
}

function buildGraph(
  snapshot: RelationshipGraphSnapshot,
  onLayoutStateChange: (running: boolean) => void,
  palette: TipxRenderPalette
) {
  const graph = new Graph({ type: "mixed", multi: true })

  snapshot.nodes.forEach((node, index) => {
    const [fallbackX, fallbackY] = deterministicPosition(index, snapshot.nodes.length)
    graph.addNode(node.id, {
      label: node.label,
      x: finiteOr(node.x, fallbackX),
      y: finiteOr(node.y, fallbackY),
      size: nodeSize(node),
      color: nodeColor(node, palette),
    })
  })

  for (const edge of snapshot.edges) {
    const attributes = {
      size: Math.max(0.6, Math.min(3, edge.weight ?? 1)),
      color: edgeColor(edge.kind, palette),
    }

    if (edge.directed || edge.kind === "transform" || edge.kind === "merger") {
      graph.addDirectedEdgeWithKey(
        edge.id,
        edge.source,
        edge.target,
        attributes
      )
    } else {
      graph.addUndirectedEdgeWithKey(
        edge.id,
        edge.source,
        edge.target,
        attributes
      )
    }
  }

  const requiresClientLayout =
    snapshot.layout === "client" &&
    snapshot.nodes.length > 2 &&
    snapshot.nodes.length <= MAX_CLIENT_FORCE_NODES

  if (requiresClientLayout) {
    onLayoutStateChange(true)
    try {
      forceAtlas2.assign(graph, {
        iterations: Math.min(80, 24 + Math.ceil(snapshot.nodes.length / 12)),
        settings: forceAtlas2.inferSettings(graph),
      })
    } finally {
      onLayoutStateChange(false)
    }
  }

  return graph
}

function deterministicPosition(index: number, count: number): [number, number] {
  const angle = (Math.PI * 2 * index) / Math.max(1, count)
  const ring = 1 + Math.floor(index / 180) * 0.28
  return [Math.cos(angle) * ring, Math.sin(angle) * ring]
}

function finiteOr(value: number | null | undefined, fallback: number) {
  return typeof value === "number" && Number.isFinite(value) ? value : fallback
}

function nodeSize(node: RelationshipNode) {
  return Math.min(18, 5 + Math.log2(node.degree + 1) * 2.2)
}

function nodeColor(node: RelationshipNode, palette: TipxRenderPalette) {
  if (node.kind === "person") return palette.graph.nodePerson
  if (node.tipStatus === "active") return palette.entity.active
  if (node.tipStatus === "expired") return palette.entity.expired
  if (node.tipStatus === "prospect") return palette.entity.prospect
  return palette.graph.nodeCompany
}

function edgeColor(kind: string, palette: TipxRenderPalette) {
  if (kind === "transform" || kind === "merger") return palette.graph.path
  if (kind === "shared-director") return palette.graph.edge
  return palette.graph.edgeMuted
}
