"use client"

import { Monitor, Moon, Sun } from "lucide-react"

import {
  TIPX_APPEARANCE_MODES,
  type TipxAppearanceMode,
  useTipxTheme,
} from "@/hooks/use-tipx-theme"
import { cn } from "@/lib/utils"

const APPEARANCE_META: Record<
  TipxAppearanceMode,
  { label: string; icon: typeof Monitor }
> = {
  system: { label: "System appearance", icon: Monitor },
  light: { label: "Light appearance", icon: Sun },
  dark: { label: "Dark appearance", icon: Moon },
}

export function AppearanceControl() {
  const { appearance, mounted, setAppearance } = useTipxTheme()

  return (
    <div
      className="mt-2 flex items-center rounded-xl border border-tipx-line/12 bg-tipx-floating/82 p-1 shadow-lg shadow-black/10 backdrop-blur-xl"
      aria-label="Appearance"
      role="group"
    >
      {TIPX_APPEARANCE_MODES.map((mode) => {
        const { label, icon: Icon } = APPEARANCE_META[mode]
        const active = mounted && appearance === mode

        return (
          <button
            key={mode}
            type="button"
            onClick={() => setAppearance(mode)}
            aria-label={label}
            aria-pressed={active}
            title={label}
            className={cn(
              "grid size-7 place-items-center rounded-lg transition",
              active
                ? "bg-tipx-brand text-tipx-on-brand shadow-sm"
                : "text-tipx-ink/38 hover:bg-tipx-ink/6 hover:text-tipx-ink/75"
            )}
          >
            <Icon className="size-3.5" aria-hidden />
          </button>
        )
      })}
    </div>
  )
}
