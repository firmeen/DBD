import type {
  RelationshipEdge,
  RelationshipGraphSnapshot,
} from "./types"

export function shortestRelationshipPath(
  snapshot: Pick<RelationshipGraphSnapshot, "nodes" | "edges">,
  sourceId: string,
  targetId: string
): string[] {
  if (sourceId === targetId) return [sourceId]

  const nodeIds = new Set(snapshot.nodes.map((node) => node.id))
  if (!nodeIds.has(sourceId) || !nodeIds.has(targetId)) return []

  const adjacency = adjacencyMap(snapshot.edges)
  const queue = [sourceId]
  const visited = new Set([sourceId])
  const previous = new Map<string, string>()

  while (queue.length > 0) {
    const current = queue.shift()
    if (!current) break

    for (const neighbour of adjacency.get(current) ?? []) {
      if (visited.has(neighbour)) continue
      visited.add(neighbour)
      previous.set(neighbour, current)

      if (neighbour === targetId) {
        return reconstructPath(previous, sourceId, targetId)
      }

      queue.push(neighbour)
    }
  }

  return []
}

export function relationshipNeighbourhood(
  snapshot: Pick<RelationshipGraphSnapshot, "nodes" | "edges">,
  focusId: string,
  hopLimit = 1,
  nodeLimit = 160
) {
  const nodeIds = new Set(snapshot.nodes.map((node) => node.id))
  if (!nodeIds.has(focusId)) {
    return { nodeIds: new Set<string>(), edgeIds: new Set<string>() }
  }

  const adjacency = adjacencyMap(snapshot.edges)
  const distance = new Map<string, number>([[focusId, 0]])
  const queue = [focusId]
  const ordered = [focusId]

  while (queue.length > 0 && ordered.length < nodeLimit) {
    const current = queue.shift()
    if (!current) break
    const currentDistance = distance.get(current) ?? 0
    if (currentDistance >= hopLimit) continue

    for (const neighbour of adjacency.get(current) ?? []) {
      if (distance.has(neighbour)) continue
      distance.set(neighbour, currentDistance + 1)
      queue.push(neighbour)
      ordered.push(neighbour)
      if (ordered.length >= nodeLimit) break
    }
  }

  const visibleNodes = new Set(ordered)
  const visibleEdges = new Set(
    snapshot.edges
      .filter(
        (edge) =>
          visibleNodes.has(edge.source) && visibleNodes.has(edge.target)
      )
      .map((edge) => edge.id)
  )

  return { nodeIds: visibleNodes, edgeIds: visibleEdges }
}

export function pathEdgeIds(
  edges: RelationshipEdge[],
  nodePath: string[]
): Set<string> {
  if (nodePath.length < 2) return new Set()

  const pairs = new Set<string>()
  for (let index = 0; index < nodePath.length - 1; index += 1) {
    const left = nodePath[index]
    const right = nodePath[index + 1]
    pairs.add(left + "::" + right)
    pairs.add(right + "::" + left)
  }

  return new Set(
    edges
      .filter((edge) => pairs.has(edge.source + "::" + edge.target))
      .map((edge) => edge.id)
  )
}

function adjacencyMap(edges: RelationshipEdge[]) {
  const adjacency = new Map<string, Set<string>>()

  for (const edge of edges) {
    addNeighbour(adjacency, edge.source, edge.target)
    addNeighbour(adjacency, edge.target, edge.source)
  }

  return adjacency
}

function addNeighbour(
  adjacency: Map<string, Set<string>>,
  source: string,
  target: string
) {
  const neighbours = adjacency.get(source) ?? new Set<string>()
  neighbours.add(target)
  adjacency.set(source, neighbours)
}

function reconstructPath(
  previous: Map<string, string>,
  sourceId: string,
  targetId: string
) {
  const path = [targetId]
  let current = targetId

  while (current !== sourceId) {
    const parent = previous.get(current)
    if (!parent) return []
    path.push(parent)
    current = parent
  }

  return path.reverse()
}
