"use client"

import * as React from "react"
import { useTheme } from "next-themes"

import {
  TIPX_RENDER_PALETTES,
  type TipxResolvedTheme,
} from "@/lib/theme/palette"

export const TIPX_APPEARANCE_MODES = ["system", "light", "dark"] as const

export type TipxAppearanceMode = (typeof TIPX_APPEARANCE_MODES)[number]

function isAppearanceMode(value: string | undefined): value is TipxAppearanceMode {
  return Boolean(
    value && TIPX_APPEARANCE_MODES.includes(value as TipxAppearanceMode)
  )
}

export function useTipxTheme() {
  const { theme, resolvedTheme, setTheme } = useTheme()
  const [mounted, setMounted] = React.useState(false)

  React.useEffect(() => {
    setMounted(true)
  }, [])

  const appearance: TipxAppearanceMode =
    mounted && isAppearanceMode(theme) ? theme : "system"
  const resolved: TipxResolvedTheme =
    mounted && resolvedTheme === "light" ? "light" : "dark"

  const setAppearance = React.useCallback(
    (mode: TipxAppearanceMode) => setTheme(mode),
    [setTheme]
  )

  return {
    mounted,
    appearance,
    resolved,
    palette: TIPX_RENDER_PALETTES[resolved],
    setAppearance,
  }
}
