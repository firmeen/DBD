"use client"

import { X } from "lucide-react"
import { AnimatePresence, motion, useReducedMotion } from "motion/react"

import { useWorkspaceStore } from "@/stores/workspace-store"

export function EntityDock() {
  const reduceMotion = useReducedMotion()
  const activeSurface = useWorkspaceStore((state) => state.activeSurface)
  const focus = useWorkspaceStore((state) => state.focus)
  const setFocus = useWorkspaceStore((state) => state.setFocus)
  const toggleCompare = useWorkspaceStore((state) => state.toggleCompare)
  const compare = useWorkspaceStore((state) => state.compare)

  if (activeSurface === "C" && focus?.kind === "company") return null

  const inCompare =
    focus &&
    compare.some(
      (entity) => entity.kind === focus.kind && entity.id === focus.id
    )

  return (
    <AnimatePresence>
      {focus ? (
        <motion.aside
          initial={reduceMotion ? false : { opacity: 0, x: 24 }}
          animate={{ opacity: 1, x: 0 }}
          exit={reduceMotion ? { opacity: 0 } : { opacity: 0, x: 18 }}
          transition={{ duration: 0.2, ease: [0.2, 0.8, 0.2, 1] }}
          className="pointer-events-auto fixed bottom-20 right-5 top-44 z-30 flex w-[min(360px,calc(100vw-40px))] flex-col rounded-2xl border border-white/10 bg-[#0b0f13]/92 shadow-[0_30px_90px_rgba(0,0,0,.4)] backdrop-blur-2xl sm:right-7"
        >
          <div className="flex items-start justify-between border-b border-white/8 p-4">
            <div>
              <p className="text-[9px] font-semibold uppercase tracking-[0.2em] text-white/30">
                Focus · {focus.kind}
              </p>
              <h2 className="mt-1 text-sm font-semibold text-white">
                {focus.label}
              </h2>
              {focus.secondary ? (
                <p className="mt-1 text-[11px] text-white/38">
                  {focus.secondary}
                </p>
              ) : null}
            </div>
            <button
              type="button"
              onClick={() => setFocus(null)}
              className="rounded-lg p-2 text-white/35 transition hover:bg-white/6 hover:text-white"
              aria-label="Close entity context"
            >
              <X className="size-3.5" />
            </button>
          </div>

          <div className="flex flex-1 items-center justify-center p-6 text-center">
            <div>
              <p className="text-xs font-medium text-white/58">
                Entity context contract ready
              </p>
              <p className="mx-auto mt-2 max-w-60 text-[11px] leading-5 text-white/30">
                Domain-specific dossier content is intentionally owned by the
                Company and People implementation tracks.
              </p>
            </div>
          </div>

          <div className="border-t border-white/8 p-3">
            <button
              type="button"
              onClick={() => toggleCompare(focus)}
              className="w-full rounded-xl border border-white/9 bg-white/[0.035] px-3 py-2.5 text-xs font-medium text-white/66 transition hover:bg-white/[0.07] hover:text-white"
            >
              {inCompare ? "Remove from compare" : "Add to compare"}
            </button>
          </div>
        </motion.aside>
      ) : null}
    </AnimatePresence>
  )
}
