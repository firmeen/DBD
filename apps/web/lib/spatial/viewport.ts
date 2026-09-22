import { WebMercatorViewport } from "@deck.gl/core"

import type { SpatialBounds, SpatialViewState } from "./types"

export function spatialBoundsForView(
  viewState: SpatialViewState,
  width: number,
  height: number
): SpatialBounds | null {
  if (width <= 0 || height <= 0) return null

  const viewport = new WebMercatorViewport({
    ...viewState,
    width,
    height,
  })

  const corners = [
    viewport.unproject([0, 0]),
    viewport.unproject([width, 0]),
    viewport.unproject([width, height]),
    viewport.unproject([0, height]),
  ]

  const longitudes = corners.map(([longitude]) => longitude)
  const latitudes = corners.map(([, latitude]) => latitude)

  return [
    Math.max(-180, Math.min(...longitudes)),
    Math.max(-85, Math.min(...latitudes)),
    Math.min(180, Math.max(...longitudes)),
    Math.min(85, Math.max(...latitudes)),
  ]
}
