"use client"

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
  const mounted = theme !== undefined
  const appearance: TipxAppearanceMode =
    isAppearanceMode(theme) ? theme : "system"
  const resolved: TipxResolvedTheme =
    resolvedTheme === "light" ? "light" : "dark"

  return {
    mounted,
    appearance,
    resolved,
    palette: TIPX_RENDER_PALETTES[resolved],
    setAppearance: (mode: TipxAppearanceMode) => setTheme(mode),
  }
}
