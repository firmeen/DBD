"use client"

import { X } from "lucide-react"
import { AnimatePresence, motion, useReducedMotion } from "motion/react"

import { useWorkspaceStore } from "@/stores/workspace-store"

export function CompareTray() {
  const reduceMotion = useReducedMotion()
  const compare = useWorkspaceStore((state) => state.compare)
  const toggleCompare = useWorkspaceStore((state) => state.toggleCompare)
  const clearCompare = useWorkspaceStore((state) => state.clearCompare)

  return (
    <AnimatePresence>
      {compare.length > 0 ? (
        <motion.div
          initial={
            reduceMotion ? false : { opacity: 0, y: 14, scale: 0.99 }
          }
          animate={{ opacity: 1, y: 0, scale: 1 }}
          exit={reduceMotion ? { opacity: 0 } : { opacity: 0, y: 10 }}
          className="pointer-events-auto fixed bottom-20 left-1/2 z-[35] flex max-w-[min(720px,calc(100vw-40px))] -translate-x-1/2 items-center gap-2 rounded-2xl border border-white/10 bg-[#0b0f13]/94 p-2 shadow-2xl shadow-black/30 backdrop-blur-xl"
        >
          <div className="px-2 text-[9px] font-semibold uppercase tracking-[0.18em] text-white/34">
            Compare
          </div>
          <div className="flex min-w-0 gap-1 overflow-x-auto">
            {compare.map((entity) => (
              <button
                key={`${entity.kind}:${entity.id}`}
                type="button"
                onClick={() => toggleCompare(entity)}
                className="flex shrink-0 items-center gap-1.5 rounded-lg border border-white/8 bg-white/[0.035] px-2.5 py-2 text-[10px] text-white/62 hover:bg-white/[0.07] hover:text-white"
              >
                <span className="max-w-32 truncate">{entity.label}</span>
                <X className="size-3 text-white/30" />
              </button>
            ))}
          </div>
          <button
            type="button"
            onClick={clearCompare}
            className="shrink-0 rounded-lg px-2 py-2 text-[9px] uppercase tracking-[0.12em] text-white/28 hover:bg-white/5 hover:text-white/60"
          >
            Clear
          </button>
        </motion.div>
      ) : null}
    </AnimatePresence>
  )
}
