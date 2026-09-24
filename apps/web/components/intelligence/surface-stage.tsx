"use client"

import dynamic from "next/dynamic"
import {
  ArrowUpRight,
  CircleDot,
  Command,
  Filter,
  Orbit,
} from "lucide-react"
import { AnimatePresence, motion, useReducedMotion } from "motion/react"

import { SURFACE_BY_ID } from "@/lib/intelligence/surfaces"
import { useWorkspaceStore } from "@/stores/workspace-store"

const SpatialSurface = dynamic(
  () =>
    import("@/components/spatial/spatial-surface").then(
      (module) => module.SpatialSurface
    ),
  {
    ssr: false,
    loading: () => <SpatialSurfaceLoader />,
  }
)

const CompanySurface = dynamic(
  () =>
    import("@/components/company/company-surface").then(
      (module) => module.CompanySurface
    ),
  {
    ssr: false,
    loading: () => <CompanySurfaceLoader />,
  }
)

export function SurfaceStage() {
  const reduceMotion = useReducedMotion()
  const activeSurface = useWorkspaceStore((state) => state.activeSurface)
  const focus = useWorkspaceStore((state) => state.focus)
  const setCommandOpen = useWorkspaceStore((state) => state.setCommandOpen)
  const setLensOpen = useWorkspaceStore((state) => state.setLensOpen)
  const surface = SURFACE_BY_ID[activeSurface]

  if (activeSurface === "M") {
    return <SpatialSurface />
  }

  if (activeSurface === "C") {
    const companySurfaceKey =
      focus?.kind === "company" ? `company:${focus.id}` : "company:discovery"

    return <CompanySurface key={companySurfaceKey} />
  }

  return (
    <main className="relative min-h-svh overflow-hidden bg-[#070a0d] text-white">
      <div className="tipx-grid absolute inset-0 opacity-45" />
      <div className="tipx-vignette absolute inset-0" />
      <div className="absolute left-[11%] top-[18%] size-[38vw] max-h-[640px] max-w-[640px] rounded-full bg-white/[0.018] blur-3xl" />
      <div className="absolute bottom-[8%] right-[13%] size-[28vw] max-h-[480px] max-w-[480px] rounded-full bg-cyan-400/[0.018] blur-3xl" />

      <section className="relative z-10 flex min-h-svh items-center px-6 py-24 sm:px-10 lg:px-16">
        <AnimatePresence mode="wait">
          <motion.div
            key={activeSurface}
            initial={reduceMotion ? false : { opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            exit={reduceMotion ? { opacity: 0 } : { opacity: 0, y: -8 }}
            transition={{ duration: 0.22, ease: [0.2, 0.8, 0.2, 1] }}
            className="max-w-3xl"
          >
            <div className="mb-7 flex items-center gap-3">
              <span className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/[0.025] px-3 py-1.5 text-[9px] font-semibold uppercase tracking-[0.22em] text-white/44 backdrop-blur">
                <CircleDot
                  className="size-3 text-emerald-400"
                  aria-hidden
                />
                Runtime foundation online
              </span>
              <span className="text-[9px] font-mono uppercase tracking-[0.18em] text-white/22">
                PR1 · Issue #28
              </span>
            </div>

            <p className="font-mono text-[11px] uppercase tracking-[0.32em] text-white/28">
              {activeSurface} / {surface.shortName}
            </p>
            <h1 className="mt-4 max-w-3xl text-balance text-4xl font-semibold leading-[1.06] tracking-[-0.045em] text-white sm:text-6xl">
              {surface.name}
            </h1>
            <p className="mt-5 max-w-2xl text-pretty text-sm leading-7 text-white/42 sm:text-[15px]">
              {surface.description} The shared TIPX runtime is active; this
              domain surface remains intentionally staged for Issue #
              {surface.ownerIssue}.
            </p>

            <div className="mt-10 flex flex-wrap gap-2">
              <RuntimeCapability icon={Orbit} label="Surface registry" />
              <RuntimeCapability icon={Filter} label="Global Lens" />
              <RuntimeCapability icon={Command} label="Command system" />
            </div>

            <div className="mt-10 flex flex-wrap gap-3">
              <button
                type="button"
                onClick={() => setCommandOpen(true)}
                className="group inline-flex h-11 items-center gap-3 rounded-xl bg-white px-4 text-xs font-semibold text-black transition hover:bg-white/90"
              >
                Open command center
                <ArrowUpRight className="size-3.5 transition-transform group-hover:translate-x-0.5 group-hover:-translate-y-0.5" />
              </button>
              <button
                type="button"
                onClick={() => setLensOpen(true)}
                className="inline-flex h-11 items-center rounded-xl border border-white/10 bg-white/[0.025] px-4 text-xs font-medium text-white/58 transition hover:border-white/18 hover:bg-white/[0.055] hover:text-white"
              >
                Define analytical Lens
              </button>
            </div>
          </motion.div>
        </AnimatePresence>
      </section>

      <div className="absolute bottom-7 right-7 z-10 hidden text-right text-[9px] uppercase tracking-[0.18em] text-white/18 lg:block">
        <p>One context · multiple representations</p>
        <p className="mt-1">Heavy engines lazy by surface</p>
      </div>
    </main>
  )
}

function RuntimeCapability({
  icon: Icon,
  label,
}: {
  icon: typeof Orbit
  label: string
}) {
  return (
    <span className="inline-flex items-center gap-2 rounded-xl border border-white/8 bg-white/[0.02] px-3 py-2 text-[10px] text-white/38">
      <Icon className="size-3.5" aria-hidden />
      {label}
    </span>
  )
}


function SpatialSurfaceLoader() {
  return (
    <main className="relative grid min-h-svh place-items-center overflow-hidden bg-[#070a0d] text-white">
      <div className="tipx-grid absolute inset-0 opacity-35" />
      <div className="tipx-vignette absolute inset-0" />
      <div className="relative z-10 flex items-center gap-3 rounded-2xl border border-white/8 bg-white/[0.025] px-4 py-3 text-[10px] font-semibold uppercase tracking-[0.2em] text-white/38 backdrop-blur-xl">
        <span className="size-1.5 animate-pulse rounded-full bg-cyan-200/70" />
        Loading spatial engine
      </div>
    </main>
  )
}


function CompanySurfaceLoader() {
  return (
    <main className="relative grid min-h-svh place-items-center overflow-hidden bg-[#070a0d] text-white">
      <div className="tipx-grid absolute inset-0 opacity-25" />
      <div className="tipx-vignette absolute inset-0" />
      <div className="relative z-10 flex items-center gap-3 rounded-2xl border border-white/8 bg-white/[0.025] px-4 py-3 text-[10px] font-semibold uppercase tracking-[0.2em] text-white/38 backdrop-blur-xl">
        <span className="size-1.5 animate-pulse rounded-full bg-cyan-200/70" />
        Loading company intelligence
      </div>
    </main>
  )
}
