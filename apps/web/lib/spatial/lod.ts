import type { SpatialLod } from "./types"

export function spatialLodForZoom(zoom: number): SpatialLod {
  if (zoom < 6) return "national"
  if (zoom < 9) return "regional"
  if (zoom < 13) return "local"
  return "entity"
}

export function h3ResolutionForZoom(zoom: number) {
  if (zoom < 6) return 4
  if (zoom < 9) return 6
  if (zoom < 13) return 8
  return 10
}

export function spatialLodLabel(lod: SpatialLod) {
  switch (lod) {
    case "national":
      return "National aggregate"
    case "regional":
      return "Regional H3"
    case "local":
      return "Local clusters"
    case "entity":
      return "Company entities"
  }
}
