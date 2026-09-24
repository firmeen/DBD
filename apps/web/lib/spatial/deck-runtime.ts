"use client"

import * as React from "react"

export interface DeckRuntime {
  DeckGL: typeof import("@deck.gl/react").DeckGL
  H3HexagonLayer: typeof import("@deck.gl/geo-layers").H3HexagonLayer
  GeoJsonLayer: typeof import("@deck.gl/layers").GeoJsonLayer
  IconLayer: typeof import("@deck.gl/layers").IconLayer
  ScatterplotLayer: typeof import("@deck.gl/layers").ScatterplotLayer
  TextLayer: typeof import("@deck.gl/layers").TextLayer
}

declare global {
  var __tipxDeckRuntime: DeckRuntime | undefined
  var __tipxDeckRuntimePromise: Promise<DeckRuntime> | undefined
}

export function loadDeckRuntime() {
  if (globalThis.__tipxDeckRuntime) {
    return Promise.resolve(globalThis.__tipxDeckRuntime)
  }

  if (!globalThis.__tipxDeckRuntimePromise) {
    globalThis.__tipxDeckRuntimePromise = Promise.all([
      import("@deck.gl/react"),
      import("@deck.gl/geo-layers"),
      import("@deck.gl/layers"),
    ])
      .then(([reactModule, geoModule, layersModule]) => {
        const runtime: DeckRuntime = {
          DeckGL: reactModule.DeckGL,
          H3HexagonLayer: geoModule.H3HexagonLayer,
          GeoJsonLayer: layersModule.GeoJsonLayer,
          IconLayer: layersModule.IconLayer,
          ScatterplotLayer: layersModule.ScatterplotLayer,
          TextLayer: layersModule.TextLayer,
        }

        globalThis.__tipxDeckRuntime = runtime
        return runtime
      })
      .catch((error) => {
        globalThis.__tipxDeckRuntimePromise = undefined
        throw error
      })
  }

  return globalThis.__tipxDeckRuntimePromise
}

export function useDeckRuntime() {
  const [runtime, setRuntime] = React.useState<DeckRuntime | null>(
    () => globalThis.__tipxDeckRuntime ?? null
  )

  React.useEffect(() => {
    if (runtime) return

    let active = true

    void loadDeckRuntime().then((loaded) => {
      if (active) setRuntime(loaded)
    })

    return () => {
      active = false
    }
  }, [runtime])

  return runtime
}
