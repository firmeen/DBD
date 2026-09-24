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

const RelationshipSurface = dynamic(
  () =>
    import("@/components/relationship/relationship-surface").then(
      (module) => module.RelationshipSurface
    ),
  {
    ssr: false,
    loading: () => <RelationshipSurfaceLoader />,
  }
)

const PersonSurface = dynamic(
  () =>
    import("@/components/people/person-surface").then(
      (module) => module.PersonSurface
    ),
  {
    ssr: false,
    loading: () => <PeopleSurfaceLoader />,
  }
)

const AnalyticsSurface = dynamic(
  () =>
    import("@/components/analytics/analytics-surface").then(
      (module) => module.AnalyticsSurface
    ),
  {
    ssr: false,
    loading: () => <AnalyticsSurfaceLoader />,
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

  if (activeSurface === "P") {
    const peopleSurfaceKey =
      focus?.kind === "person"
        ? `person:${focus.id}`
        : focus?.kind === "company"
          ? `company-board:${focus.id}`
          : "people:discovery"

    return <PersonSurface key={peopleSurfaceKey} />
  }

  if (activeSurface === "R") {
    return <RelationshipSurface />
  }

  if (activeSurface === "A" || activeSurface === "O") {
    return (
      <AnalyticsSurface
        key={`analytics:${activeSurface}`}
        mode={activeSurface === "A" ? "population" : "opportunity"}
      />
    )
  }

  return (
    <main className="relative min-h-svh overflow-hidden bg-tipx-canvas text-tipx-ink">
      <div className="tipx-grid absolute inset-0 opacity-45" />
      <div className="tipx-vignette absolute inset-0" />
      <div className="absolute left-[11%] top-[18%] size-[38vw] max-h-[640px] max-w-[640px] rounded-full bg-tipx-ink/[0.018] blur-3xl" />
      <div className="absolute bottom-[8%] right-[13%] size-[28vw] max-h-[480px] max-w-[480px] rounded-full bg-tipx-brand/[0.035] blur-3xl" />

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
              <span className="inline-flex items-center gap-2 rounded-full border border-tipx-line/10 bg-tipx-ink/[0.025] px-3 py-1.5 text-[9px] font-semibold uppercase tracking-[0.22em] text-tipx-ink/44 backdrop-blur">
                <CircleDot
                  className="size-3 text-emerald-400"
                  aria-hidden
                />
                Runtime foundation online
              </span>
              <span className="text-[9px] font-mono uppercase tracking-[0.18em] text-tipx-ink/22">
                PR1 · Issue #28
              </span>
            </div>

            <p className="font-mono text-[11px] uppercase tracking-[0.32em] text-tipx-ink/28">
              {activeSurface} / {surface.shortName}
            </p>
            <h1 className="mt-4 max-w-3xl text-balance text-4xl font-semibold leading-[1.06] tracking-[-0.045em] text-tipx-ink sm:text-6xl">
              {surface.name}
            </h1>
            <p className="mt-5 max-w-2xl text-pretty text-sm leading-7 text-tipx-ink/42 sm:text-[15px]">
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
                className="group inline-flex h-11 items-center gap-3 rounded-xl bg-tipx-brand px-4 text-xs font-semibold text-tipx-on-brand transition hover:bg-tipx-brand-strong"
              >
                Open command center
                <ArrowUpRight className="size-3.5 transition-transform group-hover:translate-x-0.5 group-hover:-translate-y-0.5" />
              </button>
              <button
                type="button"
                onClick={() => setLensOpen(true)}
                className="inline-flex h-11 items-center rounded-xl border border-tipx-line/10 bg-tipx-ink/[0.025] px-4 text-xs font-medium text-tipx-ink/58 transition hover:border-tipx-line/18 hover:bg-tipx-ink/[0.055] hover:text-tipx-ink"
              >
                Define analytical Lens
              </button>
            </div>
          </motion.div>
        </AnimatePresence>
      </section>

      <div className="absolute bottom-7 right-7 z-10 hidden text-right text-[9px] uppercase tracking-[0.18em] text-tipx-ink/18 lg:block">
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
    <span className="inline-flex items-center gap-2 rounded-xl border border-tipx-line/8 bg-tipx-ink/[0.02] px-3 py-2 text-[10px] text-tipx-ink/38">
      <Icon className="size-3.5" aria-hidden />
      {label}
    </span>
  )
}


function SpatialSurfaceLoader() {
  return (
    <main className="relative grid min-h-svh place-items-center overflow-hidden bg-tipx-canvas text-tipx-ink">
      <div className="tipx-grid absolute inset-0 opacity-35" />
      <div className="tipx-vignette absolute inset-0" />
      <div className="relative z-10 flex items-center gap-3 rounded-2xl border border-tipx-line/8 bg-tipx-ink/[0.025] px-4 py-3 text-[10px] font-semibold uppercase tracking-[0.2em] text-tipx-ink/38 backdrop-blur-xl">
        <span className="size-1.5 animate-pulse rounded-full bg-tipx-brand/70" />
        Loading spatial engine
      </div>
    </main>
  )
}


function CompanySurfaceLoader() {
  return (
    <main className="relative grid min-h-svh place-items-center overflow-hidden bg-tipx-canvas text-tipx-ink">
      <div className="tipx-grid absolute inset-0 opacity-25" />
      <div className="tipx-vignette absolute inset-0" />
      <div className="relative z-10 flex items-center gap-3 rounded-2xl border border-tipx-line/8 bg-tipx-ink/[0.025] px-4 py-3 text-[10px] font-semibold uppercase tracking-[0.2em] text-tipx-ink/38 backdrop-blur-xl">
        <span className="size-1.5 animate-pulse rounded-full bg-tipx-brand/70" />
        Loading company intelligence
      </div>
    </main>
  )
}


function PeopleSurfaceLoader() {
  return (
    <main className="relative grid min-h-svh place-items-center overflow-hidden bg-tipx-canvas text-tipx-ink">
      <div className="tipx-grid absolute inset-0 opacity-24" />
      <div className="tipx-vignette absolute inset-0" />
      <div className="relative z-10 flex items-center gap-3 rounded-2xl border border-tipx-line/8 bg-tipx-ink/[0.025] px-4 py-3 text-[10px] font-semibold uppercase tracking-[0.2em] text-tipx-ink/38 backdrop-blur-xl">
        <span className="size-1.5 animate-pulse rounded-full bg-tipx-brand/70" />
        Loading people intelligence
      </div>
    </main>
  )
}


function RelationshipSurfaceLoader() {
  return (
    <main className="relative grid min-h-svh place-items-center overflow-hidden bg-tipx-canvas text-tipx-ink">
      <div className="tipx-grid absolute inset-0 opacity-18" />
      <div className="tipx-vignette absolute inset-0" />
      <div className="relative z-10 flex items-center gap-3 rounded-2xl border border-tipx-line/8 bg-tipx-ink/[0.025] px-4 py-3 text-[10px] font-semibold uppercase tracking-[0.2em] text-tipx-ink/38 backdrop-blur-xl">
        <span className="size-1.5 animate-pulse rounded-full bg-tipx-brand/70" />
        Loading relationship observatory
      </div>
    </main>
  )
}


function AnalyticsSurfaceLoader() {
  return (
    <main className="relative grid min-h-svh place-items-center overflow-hidden bg-tipx-canvas text-tipx-ink">
      <div className="tipx-grid absolute inset-0 opacity-24" />
      <div className="tipx-vignette absolute inset-0" />
      <div className="relative z-10 flex items-center gap-3 rounded-2xl border border-tipx-line/8 bg-tipx-ink/[0.025] px-4 py-3 text-[10px] font-semibold uppercase tracking-[0.2em] text-tipx-ink/38 backdrop-blur-xl">
        <span className="size-1.5 animate-pulse rounded-full bg-emerald-200/70" />
        Loading analytical field
      </div>
    </main>
  )
}
