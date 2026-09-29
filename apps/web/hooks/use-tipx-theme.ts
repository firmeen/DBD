"use client"

import * as React from "react"
import { useTheme } from "next-themes"

import {
  TIPX_RENDER_PALETTES,
  type TipxResolvedTheme,
} from "@/lib/theme/palette"

export const TIPX_APPEARANCE_MODES = ["system", "light", "dark"] as const

export type TipxAppearanceMode = (typeof TIPX_APPEARANCE_MODES)[number]

const subscribeHydration = () => () => {}

function isAppearanceMode(value: string | undefined): value is TipxAppearanceMode {
  return Boolean(
    value && TIPX_APPEARANCE_MODES.includes(value as TipxAppearanceMode)
  )
}

export function resolveTipxThemeSnapshot({
  theme,
  resolvedTheme,
  mounted,
}: {
  theme: string | undefined
  resolvedTheme: string | undefined
  mounted: boolean
}) {
  const appearance: TipxAppearanceMode =
    mounted && isAppearanceMode(theme) ? theme : "system"
  const resolved: TipxResolvedTheme =
    mounted && resolvedTheme === "light" ? "light" : "dark"

  return {
    appearance,
    resolved,
    palette: TIPX_RENDER_PALETTES[resolved],
  }
}

export function useTipxTheme() {
  const { theme, resolvedTheme, setTheme } = useTheme()

  const mounted = React.useSyncExternalStore(
    subscribeHydration,
    () => true,
    () => false
  )

  const snapshot = resolveTipxThemeSnapshot({
    theme,
    resolvedTheme,
    mounted,
  })

  return {
    mounted,
    ...snapshot,
    setAppearance: (mode: TipxAppearanceMode) => setTheme(mode),
  }
}
