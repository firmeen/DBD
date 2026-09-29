"use client"

import * as React from "react"
import { graphlib, layout } from "@dagrejs/dagre"
import { GitMerge } from "lucide-react"

import { useTipxTheme } from "@/hooks/use-tipx-theme"
import type { GenealogySnapshot } from "@/lib/relationship/types"
import type { EntityRef } from "@/lib/intelligence/types"

const NODE_WIDTH = 190
const NODE_HEIGHT = 68

export function RelationshipGenealogy({
  snapshot,
  onFocusCompany,
}: {
  snapshot: GenealogySnapshot
  onFocusCompany: (entity: EntityRef) => void
}) {
  const { palette } = useTipxTheme()
  const drawing = React.useMemo(() => buildGenealogy(snapshot), [snapshot])

  if (snapshot.nodes.length === 0) {
    return (
      <div className="absolute inset-0 grid place-items-center px-8 text-center">
        <div>
          <GitMerge className="mx-auto size-6 text-tipx-ink/18" />
          <p className="mt-4 text-sm font-medium text-tipx-ink/49">
            Genealogy unavailable
          </p>
          <p className="mx-auto mt-2 max-w-lg text-[10px] leading-5 text-tipx-ink/25">
            {snapshot.message ||
              "Focus a company with transformation or merger history to open directed genealogy."}
          </p>
        </div>
      </div>
    )
  }

  return (
    <div className="absolute inset-0 overflow-auto px-6 pb-24 pt-40">
      <div
        className="relative mx-auto min-h-full min-w-full"
        style={{
          width: Math.max(drawing.width, 900),
          height: Math.max(drawing.height, 520),
        }}
      >
        <svg
          width={drawing.width}
          height={drawing.height}
          viewBox={"0 0 " + drawing.width + " " + drawing.height}
          className="absolute left-1/2 top-0 -translate-x-1/2 overflow-visible"
          role="img"
          aria-label="Directed company transformation and merger genealogy"
        >
          <defs>
            <marker
              id="tipx-genealogy-arrow"
              viewBox="0 0 10 10"
              refX="8"
              refY="5"
              markerWidth="6"
              markerHeight="6"
              orient="auto-start-reverse"
            >
              <path d="M 0 0 L 10 5 L 0 10 z" fill={palette.graph.edge} />
            </marker>
          </defs>

          {drawing.edges.map((edge) => (
            <g key={edge.id}>
              <path
                d={polylinePath(edge.points)}
                fill="none"
                stroke={
                  edge.kind === "merger" ? palette.graph.path : palette.graph.edge
                }
                strokeOpacity={edge.kind === "merger" ? 0.7 : 0.5}
                strokeWidth={edge.kind === "merger" ? 2.1 : 1.4}
                markerEnd="url(#tipx-genealogy-arrow)"
              />
              {edge.label ? (
                <text
                  x={edge.labelX}
                  y={edge.labelY - 7}
                  textAnchor="middle"
                  fontSize="9"
                  fill={palette.chart.muted}
                >
                  {edge.label}
                </text>
              ) : null}
            </g>
          ))}

          {drawing.nodes.map((node) => (
            <g
              key={node.id}
              transform={
                "translate(" +
                (node.x - NODE_WIDTH / 2) +
                "," +
                (node.y - NODE_HEIGHT / 2) +
                ")"
              }
              onClick={() => onFocusCompany(node.company)}
              onKeyDown={(event) => {
                if (event.key === "Enter" || event.key === " ") {
                  event.preventDefault()
                  onFocusCompany(node.company)
                }
              }}
              className="cursor-pointer"
              role="button"
              tabIndex={0}
            >
              <rect
                width={NODE_WIDTH}
                height={NODE_HEIGHT}
                rx="15"
                fill={palette.graph.canvas}
                stroke={palette.graph.edge}
                strokeOpacity="0.28"
              />
              <text
                x="14"
                y="27"
                fontSize="10"
                fontWeight="600"
                fill={palette.graph.label}
              >
                {truncate(node.company.label, 28)}
              </text>
              <text x="14" y="46" fontSize="8" fill={palette.chart.muted}>
                {truncate(node.company.secondary || node.company.id, 35)}
              </text>
            </g>
          ))}
        </svg>
      </div>

      <div className="pointer-events-none fixed bottom-20 left-7 rounded-full border border-tipx-line/7 bg-tipx-floating/78 px-3 py-1.5 text-[8px] uppercase tracking-[0.14em] text-tipx-ink/24 backdrop-blur-xl">
        Dagre · directed transform / merger genealogy
      </div>
    </div>
  )
}

function buildGenealogy(snapshot: GenealogySnapshot) {
  const graph = new graphlib.Graph({ multigraph: true })
  graph.setGraph({
    rankdir: "LR",
    ranksep: 96,
    nodesep: 48,
    edgesep: 22,
    marginx: 36,
    marginy: 36,
  })
  graph.setDefaultEdgeLabel(() => ({}))

  for (const node of snapshot.nodes) {
    graph.setNode(node.id, {
      width: NODE_WIDTH,
      height: NODE_HEIGHT,
    })
  }

  for (const edge of snapshot.edges) {
    graph.setEdge(
      edge.source,
      edge.target,
      {
        id: edge.id,
        label: edge.label || edge.date || "",
        kind: edge.kind,
      },
      edge.id
    )
  }

  layout(graph)

  const graphShape = graph.graph()
  const nodes = snapshot.nodes.map((node) => {
    const position = graph.node(node.id)
    return {
      ...node,
      x: position?.x ?? 0,
      y: position?.y ?? 0,
    }
  })

  const edges = snapshot.edges.map((edge) => {
    const shape = graph.edge(edge.source, edge.target, edge.id)
    const points = shape?.points ?? []
    const midpoint = points[Math.floor(points.length / 2)] ?? { x: 0, y: 0 }

    return {
      ...edge,
      points,
      labelX: midpoint.x,
      labelY: midpoint.y,
    }
  })

  return {
    width: Math.ceil(graphShape.width ?? 900),
    height: Math.ceil(graphShape.height ?? 520),
    nodes,
    edges,
  }
}

function polylinePath(points: Array<{ x: number; y: number }>) {
  if (points.length === 0) return ""
  return points
    .map((point, index) =>
      (index === 0 ? "M " : "L ") + point.x + " " + point.y
    )
    .join(" ")
}

function truncate(value: string, length: number) {
  return value.length <= length ? value : value.slice(0, length - 1) + "…"
}
