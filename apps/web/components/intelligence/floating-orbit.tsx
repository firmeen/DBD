"use client"

import { useMachine } from "@xstate/react"
import { AnimatePresence, motion, useReducedMotion } from "motion/react"

import { AppearanceControl } from "./appearance-control"
import { orbitMachine } from "@/lib/intelligence/navigation-machine"
import { SURFACES } from "@/lib/intelligence/surfaces"
import type { SurfaceId } from "@/lib/intelligence/types"
import { cn } from "@/lib/utils"
import { useWorkspaceStore } from "@/stores/workspace-store"

export function FloatingOrbit() {
  const [state, send] = useMachine(orbitMachine)
  const reduceMotion = useReducedMotion()
  const activeSurface = useWorkspaceStore((value) => value.activeSurface)
  const setActiveSurface = useWorkspaceStore((value) => value.setActiveSurface)

  function activate(surface: SurfaceId) {
    setActiveSurface(surface)
    send({ type: "CLEAR_PEEK" })
  }

  return (
    <nav
      aria-label="Intelligence surfaces"
      className="pointer-events-auto fixed right-5 top-5 z-40 flex flex-col items-end gap-1.5 sm:right-7 sm:top-7"
      onMouseEnter={() => send({ type: "POINTER_ENTER" })}
      onMouseLeave={() => send({ type: "POINTER_LEAVE" })}
    >
      <div className="mb-2 flex items-center gap-2 pr-1 text-[10px] font-semibold uppercase tracking-[0.24em] text-tipx-ink/35">
        <span className="h-px w-5 bg-tipx-ink/15" />
        Surfaces
      </div>

      {SURFACES.map((surface) => {
        const isActive = activeSurface === surface.id
        const isPeeked = state.context.peekedSurface === surface.id

        return (
          <div
            key={surface.id}
            className="relative flex items-center justify-end"
          >
            <AnimatePresence>
              {isPeeked ? (
                <motion.div
                  initial={
                    reduceMotion ? false : { opacity: 0, x: 8, scale: 0.98 }
                  }
                  animate={{ opacity: 1, x: 0, scale: 1 }}
                  exit={
                    reduceMotion
                      ? { opacity: 0 }
                      : { opacity: 0, x: 6, scale: 0.98 }
                  }
                  transition={{
                    duration: 0.16,
                    ease: [0.2, 0.8, 0.2, 1],
                  }}
                  className="absolute right-12 mr-2 whitespace-nowrap rounded-xl border border-tipx-line/10 bg-tipx-floating/94 px-3 py-2 shadow-2xl shadow-black/35 backdrop-blur-xl"
                >
                  <p className="text-xs font-semibold text-tipx-ink">
                    {surface.name}
                  </p>
                  <p className="mt-0.5 max-w-72 text-[10px] leading-4 text-tipx-ink/45">
                    {surface.description}
                  </p>
                </motion.div>
              ) : null}
            </AnimatePresence>

            <button
              type="button"
              aria-label={surface.name}
              aria-current={isActive ? "page" : undefined}
              onMouseEnter={() =>
                send({ type: "PEEK", surface: surface.id })
              }
              onFocus={() => send({ type: "PEEK", surface: surface.id })}
              onBlur={() => send({ type: "CLEAR_PEEK" })}
              onClick={() => activate(surface.id)}
              className={cn(
                "group grid size-10 place-items-center rounded-xl border font-mono text-xs font-semibold transition-[transform,background-color,border-color,color,box-shadow] duration-200",
                isActive
                  ? "border-tipx-line/24 bg-tipx-brand text-tipx-on-brand shadow-[0_14px_35px_rgba(0,0,0,.28)]"
                  : "border-tipx-line/10 bg-tipx-floating/88 text-tipx-ink/62 shadow-lg shadow-black/15 backdrop-blur-xl hover:-translate-x-0.5 hover:border-tipx-line/22 hover:bg-tipx-ink/9 hover:text-tipx-ink"
              )}
            >
              {surface.id}
            </button>
          </div>
        )
      })}

      <AppearanceControl />
    </nav>
  )
}
