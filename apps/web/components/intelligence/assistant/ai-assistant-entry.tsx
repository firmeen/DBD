"use client"

import * as React from "react"
import { Sparkles } from "lucide-react"
import { motion, useReducedMotion } from "motion/react"

import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import {
  aiAssistantAmbientMotionEnabled,
  aiAssistantEntryEnabled,
} from "@/lib/assistant/config"
import {
  aiAmbientDelay,
  createAiAmbientTarget,
  type AiAmbientTarget,
} from "@/lib/assistant/motion"
import { cn } from "@/lib/utils"

const STILL_TARGET: AiAmbientTarget = {
  x: 0,
  y: 0,
  rotate: 0,
  duration: 0,
  restMs: 0,
}

export function AiAssistantEntry() {
  if (!aiAssistantEntryEnabled()) return null

  return <EnabledAiAssistantEntry />
}

function EnabledAiAssistantEntry() {
  const reduceMotion = useReducedMotion()
  const ambientMotionEnabled = aiAssistantAmbientMotionEnabled()
  const [desktopRoaming, setDesktopRoaming] = React.useState(false)
  const [tooltipOpen, setTooltipOpen] = React.useState(false)
  const [target, setTarget] =
    React.useState<AiAmbientTarget>(STILL_TARGET)
  const clickTimer = React.useRef<number | null>(null)

  React.useEffect(() => {
    const media = window.matchMedia("(min-width: 768px)")

    function sync() {
      setDesktopRoaming(media.matches)
    }

    sync()
    media.addEventListener("change", sync)
    return () => media.removeEventListener("change", sync)
  }, [])

  const canRoam =
    ambientMotionEnabled && !reduceMotion && desktopRoaming

  React.useEffect(() => {
    if (!canRoam || tooltipOpen) return

    let timer = window.setTimeout(drift, 2_800)

    function drift() {
      const next = createAiAmbientTarget()
      setTarget(next)
      timer = window.setTimeout(drift, aiAmbientDelay(next))
    }

    return () => window.clearTimeout(timer)
  }, [canRoam, tooltipOpen])

  React.useEffect(
    () => () => {
      if (clickTimer.current !== null) {
        window.clearTimeout(clickTimer.current)
      }
    },
    []
  )

  function showComingSoon() {
    setTooltipOpen(true)

    if (clickTimer.current !== null) {
      window.clearTimeout(clickTimer.current)
    }

    clickTimer.current = window.setTimeout(() => {
      setTooltipOpen(false)
      clickTimer.current = null
    }, 2_800)
  }

  return (
    <motion.div
      className="pointer-events-none fixed bottom-24 right-4 z-40 md:bottom-auto md:left-7 md:right-auto md:top-[54%]"
      animate={
        canRoam
          ? {
              x: target.x,
              y: target.y,
              rotate: target.rotate,
            }
          : { x: 0, y: 0, rotate: 0 }
      }
      transition={
        reduceMotion
          ? { duration: 0 }
          : canRoam
            ? {
                duration: Math.max(target.duration, 0.2),
                ease: [0.22, 0.75, 0.25, 1],
              }
            : { duration: 0.18, ease: "easeOut" }
      }
    >
      <TooltipProvider delay={120}>
        <Tooltip
          open={tooltipOpen}
          onOpenChange={setTooltipOpen}
        >
          <TooltipTrigger
            type="button"
            closeOnClick={false}
            aria-label="TIPX AI Assistant — coming soon"
            onClick={showComingSoon}
            className={cn(
              "group pointer-events-auto relative grid size-12 place-items-center rounded-[18px] border border-tipx-line/14 bg-tipx-floating/94 text-tipx-ink shadow-[0_18px_50px_rgba(0,0,0,.3)] backdrop-blur-2xl",
              "transition-[transform,border-color,background-color,box-shadow] duration-200 hover:scale-[1.04] hover:border-tipx-line/26 hover:bg-tipx-elevated",
              "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-tipx-brand/55 focus-visible:ring-offset-2 focus-visible:ring-offset-tipx-canvas"
            )}
          >
            <span
              aria-hidden
              className="absolute inset-[5px] rounded-[14px] border border-tipx-brand/16 bg-tipx-brand-soft/42"
            />
            <span
              aria-hidden
              className="absolute -right-1 -top-1 size-3 rounded-full border border-tipx-canvas bg-tipx-brand shadow-[0_0_18px_color-mix(in_oklab,var(--tipx-brand)_55%,transparent)]"
            />
            <Sparkles
              aria-hidden
              className="relative z-10 size-[18px] text-tipx-brand-strong transition-transform duration-200 group-hover:-rotate-6 group-hover:scale-110"
            />
            <span className="absolute bottom-[5px] right-[7px] z-10 font-mono text-[6px] font-bold uppercase tracking-[0.08em] text-tipx-ink/48">
              AI
            </span>
          </TooltipTrigger>

          <TooltipContent
            side={desktopRoaming ? "right" : "top"}
            sideOffset={10}
            align="center"
            className="max-w-[280px] items-start rounded-2xl border border-tipx-line/12 bg-tipx-floating/97 px-4 py-3 text-tipx-ink shadow-[0_22px_70px_rgba(0,0,0,.42)] backdrop-blur-2xl"
          >
            <div className="min-w-0">
              <div className="flex items-center gap-2">
                <span className="grid size-5 place-items-center rounded-md bg-tipx-brand text-tipx-on-brand">
                  <Sparkles className="size-3" aria-hidden />
                </span>
                <p className="text-[11px] font-semibold tracking-[-0.01em]">
                  TIPX AI Assistant
                </p>
              </div>
              <p className="mt-2 text-[10px] leading-5 text-tipx-ink/52">
                ผู้ช่วยวิเคราะห์ข้อมูลอัจฉริยะกำลังอยู่ระหว่างการพัฒนา
              </p>
              <p className="mt-2 font-mono text-[8px] font-semibold uppercase tracking-[0.18em] text-tipx-brand-strong/70">
                Intelligence assistance · Coming soon
              </p>
            </div>
          </TooltipContent>
        </Tooltip>
      </TooltipProvider>

      <span className="sr-only" aria-live="polite">
        {tooltipOpen
          ? "TIPX AI Assistant is currently in development."
          : ""}
      </span>
    </motion.div>
  )
}
