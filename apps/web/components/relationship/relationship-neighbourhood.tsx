"use client"

import * as React from "react"
import cytoscape from "cytoscape"
import fcose from "cytoscape-fcose"

import { useTipxTheme } from "@/hooks/use-tipx-theme"
import type { TipxRenderPalette } from "@/lib/theme/palette"
import { relationshipNeighbourhood } from "@/lib/relationship/graph-algorithms"
import type { RelationshipGraphSnapshot } from "@/lib/relationship/types"
import { relationshipNodeToEntity } from "@/lib/relationship/types"
import type { EntityRef } from "@/lib/intelligence/types"

cytoscape.use(fcose)

export function RelationshipNeighbourhood({
  snapshot,
  focusId,
  hiddenNodeIds,
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
  const cyRef = React.useRef<ReturnType<typeof cytoscape> | null>(null)

  React.useEffect(() => {
    paletteRef.current = palette
    cyRef.current?.style(neighbourhoodStyles(palette))
  }, [palette])

  React.useEffect(() => {
    const container = containerRef.current
    if (!container || !focusId) return

    const neighbourhood = relationshipNeighbourhood(
      snapshot,
      focusId,
      2,
      180
    )
    const nodeById = new Map(snapshot.nodes.map((node) => [node.id, node]))

    const elements = [
      ...snapshot.nodes
        .filter(
          (node) =>
            neighbourhood.nodeIds.has(node.id) &&
            !hiddenNodeIds.has(node.id)
        )
        .map((node) => ({
          data: {
            id: node.id,
            label: node.label,
            kind: node.kind,
            degree: node.degree,
            focus: node.id === focusId ? "true" : "false",
            path: pathNodeIds.has(node.id) ? "true" : "false",
          },
        })),
      ...snapshot.edges
        .filter(
          (edge) =>
            neighbourhood.edgeIds.has(edge.id) &&
            !hiddenNodeIds.has(edge.source) &&
            !hiddenNodeIds.has(edge.target)
        )
        .map((edge) => ({
          data: {
            id: edge.id,
            source: edge.source,
            target: edge.target,
            kind: edge.kind,
            path: pathEdgeIds.has(edge.id) ? "true" : "false",
          },
        })),
    ]

    const cy = cytoscape({
      container,
      elements,
      minZoom: 0.15,
      maxZoom: 4,
      wheelSensitivity: 0.18,
      style: neighbourhoodStyles(paletteRef.current),
    })

    cyRef.current = cy

    onLayoutStateChange(true)
    const layout = cy.layout({
      name: "fcose",
      quality: "default",
      randomize: true,
      animate: false,
      fit: true,
      padding: 56,
      nodeSeparation: 90,
      idealEdgeLength: 95,
    })

    layout.one("layoutstop", () => onLayoutStateChange(false))
    layout.run()

    cy.on("tap", "node", (event) => {
      const node = nodeById.get(event.target.id())
      if (node) onFocus(relationshipNodeToEntity(node))
    })

    cy.on("dbltap", "node", (event) => {
      const node = nodeById.get(event.target.id())
      if (node) onToggleSelection(relationshipNodeToEntity(node))
    })

    cy.on("mouseover", "node", (event) => {
      const node = nodeById.get(event.target.id())
      onHover(node ? relationshipNodeToEntity(node) : null)
    })

    cy.on("mouseout", "node", () => onHover(null))

    return () => {
      cyRef.current = null
      cy.destroy()
      onHover(null)
      onLayoutStateChange(false)
    }
  }, [
    focusId,
    hiddenNodeIds,
    onFocus,
    onHover,
    onLayoutStateChange,
    onToggleSelection,
    pathEdgeIds,
    pathNodeIds,
    snapshot,
  ])

  if (!focusId) {
    return (
      <div className="absolute inset-0 grid place-items-center px-8 text-center">
        <div>
          <p className="text-sm font-medium text-tipx-ink/50">
            Focus an entity to open Neighbourhood
          </p>
          <p className="mx-auto mt-2 max-w-md text-[10px] leading-5 text-tipx-ink/26">
            fCoSE is reserved for bounded structured investigation rather than
            the full exploratory population.
          </p>
        </div>
      </div>
    )
  }

  return (
    <div className="absolute inset-0">
      <div
        ref={containerRef}
        className="absolute inset-0"
        aria-label="Focused relationship neighbourhood"
      />
      <div className="pointer-events-none absolute bottom-5 left-5 rounded-full border border-tipx-line/7 bg-tipx-floating/76 px-3 py-1.5 text-[8px] uppercase tracking-[0.14em] text-tipx-ink/24 backdrop-blur-xl">
        fCoSE · bounded 2-hop neighbourhood
      </div>
    </div>
  )
}


function neighbourhoodStyles(palette: TipxRenderPalette) {
  return [
    {
      selector: "node",
      style: {
        label: "data(label)",
        "font-size": 9,
        color: palette.graph.label,
        "text-background-color": palette.graph.canvas,
        "text-background-opacity": 0.86,
        "text-background-padding": "3px",
        "text-margin-y": -12,
        width: "mapData(degree, 0, 30, 24, 48)",
        height: "mapData(degree, 0, 30, 24, 48)",
        "background-color": palette.graph.node,
        "border-width": 1,
        "border-color": palette.graph.label,
        "border-opacity": 0.2,
      },
    },
    {
      selector: 'node[kind = "person"]',
      style: {
        shape: "ellipse",
        "background-color": palette.graph.nodePerson,
        color: palette.graph.label,
      },
    },
    {
      selector: 'node[kind = "company"]',
      style: {
        shape: "round-rectangle",
        "background-color": palette.graph.nodeCompany,
      },
    },
    {
      selector: 'node[focus = "true"]',
      style: {
        "border-width": 4,
        "border-color": palette.graph.selected,
        "border-opacity": 0.95,
      },
    },
    {
      selector: 'node[path = "true"]',
      style: {
        "background-color": palette.graph.path,
        color: palette.graph.label,
      },
    },
    {
      selector: "edge",
      style: {
        width: 1.1,
        "line-color": palette.graph.edge,
        "target-arrow-color": palette.graph.edge,
        "curve-style": "bezier",
        opacity: 0.55,
      },
    },
    {
      selector: 'edge[kind = "transform"], edge[kind = "merger"]',
      style: {
        "target-arrow-shape": "triangle",
        "arrow-scale": 0.8,
      },
    },
    {
      selector: 'edge[path = "true"]',
      style: {
        width: 2.6,
        "line-color": palette.graph.path,
        "target-arrow-color": palette.graph.path,
        opacity: 1,
      },
    },
  ]
}
