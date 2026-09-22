"use client"

import * as React from "react"
import {
  FloatingPortal,
  autoUpdate,
  flip,
  offset,
  shift,
  useClick,
  useDismiss,
  useFloating,
  useInteractions,
  useRole,
} from "@floating-ui/react"
import { Filter, RotateCcw, SlidersHorizontal } from "lucide-react"
import { AnimatePresence, motion, useReducedMotion } from "motion/react"

import { useGlobalLens } from "@/hooks/use-global-lens"
import { cn } from "@/lib/utils"
import { useWorkspaceStore } from "@/stores/workspace-store"

const tipOptions = [
  { value: null, label: "All" },
  { value: "active", label: "Active" },
  { value: "expired", label: "Expired" },
  { value: "prospect", label: "Prospect" },
] as const

export function GlobalLens() {
  const reduceMotion = useReducedMotion()
  const externalOpen = useWorkspaceStore((state) => state.lensOpen)
  const setExternalOpen = useWorkspaceStore((state) => state.setLensOpen)
  const { lens, setLens, clearLens, activeCount } = useGlobalLens()
  const [open, setOpen] = React.useState(externalOpen)

  React.useEffect(() => setOpen(externalOpen), [externalOpen])

  const { refs, floatingStyles, context } = useFloating({
    open,
    onOpenChange(next) {
      setOpen(next)
      setExternalOpen(next)
    },
    placement: "top-start",
    whileElementsMounted: autoUpdate,
    middleware: [offset(10), flip({ padding: 12 }), shift({ padding: 12 })],
  })

  const click = useClick(context)
  const dismiss = useDismiss(context)
  const role = useRole(context, { role: "dialog" })
  const { getReferenceProps, getFloatingProps } = useInteractions([
    click,
    dismiss,
    role,
  ])

  return (
    <>
      <div className="pointer-events-auto fixed bottom-5 left-5 z-40 sm:bottom-7 sm:left-7">
        <button
          ref={refs.setReference}
          type="button"
          {...getReferenceProps()}
          className={cn(
            "group flex h-11 items-center gap-3 rounded-2xl border px-3.5 shadow-2xl shadow-black/25 backdrop-blur-xl transition-colors",
            activeCount > 0
              ? "border-white/20 bg-white text-black"
              : "border-white/10 bg-[#0b0f13]/84 text-white hover:border-white/18 hover:bg-[#10161c]/92"
          )}
        >
          <span className="grid size-6 place-items-center rounded-lg border border-current/12">
            <Filter className="size-3.5" aria-hidden />
          </span>
          <span className="text-left">
            <span className="block text-[10px] font-semibold uppercase tracking-[0.18em] opacity-55">
              Global Lens
            </span>
            <span className="block text-xs font-medium">
              {activeCount > 0
                ? `${activeCount} dimensions active`
                : "Universe · unfiltered"}
            </span>
          </span>
          <span className="ml-1 rounded-md border border-current/10 px-1.5 py-0.5 font-mono text-[9px] opacity-50">
            F
          </span>
        </button>
      </div>

      <FloatingPortal>
        <AnimatePresence>
          {open ? (
            <motion.div
              ref={refs.setFloating}
              style={floatingStyles}
              {...getFloatingProps()}
              initial={
                reduceMotion
                  ? false
                  : { opacity: 0, y: 6, scale: 0.985 }
              }
              animate={{ opacity: 1, y: 0, scale: 1 }}
              exit={
                reduceMotion
                  ? { opacity: 0 }
                  : { opacity: 0, y: 4, scale: 0.99 }
              }
              transition={{ duration: 0.16, ease: [0.2, 0.8, 0.2, 1] }}
              className="z-[80] w-[min(430px,calc(100vw-24px))] overflow-hidden rounded-2xl border border-white/12 bg-[#0b0f13]/97 shadow-[0_28px_90px_rgba(0,0,0,.55)] backdrop-blur-2xl"
            >
              <div className="flex items-start justify-between border-b border-white/8 px-5 py-4">
                <div>
                  <div className="flex items-center gap-2 text-white">
                    <SlidersHorizontal className="size-4" aria-hidden />
                    <h2 className="text-sm font-semibold">
                      Global Intelligence Lens
                    </h2>
                  </div>
                  <p className="mt-1 max-w-sm text-[11px] leading-5 text-white/40">
                    Shareable analytical scope. These dimensions follow
                    compatible surfaces instead of resetting per page.
                  </p>
                </div>
                <button
                  type="button"
                  onClick={() => void clearLens()}
                  disabled={activeCount === 0}
                  className="rounded-lg p-2 text-white/38 transition hover:bg-white/6 hover:text-white disabled:pointer-events-none disabled:opacity-20"
                  aria-label="Clear Global Lens"
                >
                  <RotateCcw className="size-3.5" />
                </button>
              </div>

              <div className="grid gap-4 p-5">
                <LensTextField
                  label="Province"
                  value={lens.province ?? ""}
                  placeholder="e.g. Bangkok"
                  onChange={(value) =>
                    void setLens({ province: value || null })
                  }
                />

                <div className="grid grid-cols-2 gap-3">
                  <LensTextField
                    label="District"
                    value={lens.district ?? ""}
                    placeholder="Any district"
                    onChange={(value) =>
                      void setLens({ district: value || null })
                    }
                  />
                  <LensTextField
                    label="Business"
                    value={lens.business ?? ""}
                    placeholder="Any business"
                    onChange={(value) =>
                      void setLens({ business: value || null })
                    }
                  />
                </div>

                <div>
                  <p className="mb-2 text-[10px] font-semibold uppercase tracking-[0.18em] text-white/35">
                    TIP Status
                  </p>
                  <div className="grid grid-cols-4 gap-1 rounded-xl border border-white/8 bg-black/20 p-1">
                    {tipOptions.map((option) => {
                      const selected = lens.tipStatus === option.value

                      return (
                        <button
                          key={option.label}
                          type="button"
                          onClick={() =>
                            void setLens({ tipStatus: option.value })
                          }
                          className={cn(
                            "rounded-lg px-2 py-2 text-[10px] font-medium transition",
                            selected
                              ? "bg-white text-black shadow-sm"
                              : "text-white/42 hover:bg-white/6 hover:text-white/72"
                          )}
                        >
                          {option.label}
                        </button>
                      )
                    })}
                  </div>
                </div>

                <LensTextField
                  label="Fiscal Year"
                  value={lens.fiscalYear?.toString() ?? ""}
                  placeholder="e.g. 2025"
                  inputMode="numeric"
                  onChange={(value) => {
                    const year = Number.parseInt(value, 10)
                    void setLens({
                      fiscalYear: Number.isFinite(year) ? year : null,
                    })
                  }}
                />
              </div>

              <div className="border-t border-white/8 bg-white/[0.018] px-5 py-3 text-[10px] leading-4 text-white/32">
                Population counts remain unavailable until a production serving
                contract supplies them. The Lens never invents analytical totals.
              </div>
            </motion.div>
          ) : null}
        </AnimatePresence>
      </FloatingPortal>
    </>
  )
}

function LensTextField({
  label,
  value,
  placeholder,
  onChange,
  inputMode,
}: {
  label: string
  value: string
  placeholder: string
  onChange: (value: string) => void
  inputMode?: React.InputHTMLAttributes<HTMLInputElement>["inputMode"]
}) {
  return (
    <label className="block">
      <span className="mb-2 block text-[10px] font-semibold uppercase tracking-[0.18em] text-white/35">
        {label}
      </span>
      <input
        value={value}
        inputMode={inputMode}
        onChange={(event) => onChange(event.target.value)}
        placeholder={placeholder}
        data-keyboard-input
        className="h-10 w-full rounded-xl border border-white/8 bg-white/[0.035] px-3 text-xs text-white outline-none transition placeholder:text-white/22 focus:border-white/22 focus:bg-white/[0.055]"
      />
    </label>
  )
}
