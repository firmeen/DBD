"use client"

import { Command, CornerDownLeft, Layers3 } from "lucide-react"

import { useGlobalLens } from "@/hooks/use-global-lens"
import { SURFACE_BY_ID } from "@/lib/intelligence/surfaces"
import { useWorkspaceStore } from "@/stores/workspace-store"

export function ContextBar() {
  const activeSurface = useWorkspaceStore((state) => state.activeSurface)
  const selection = useWorkspaceStore((state) => state.selection)
  const compare = useWorkspaceStore((state) => state.compare)
  const setCommandOpen = useWorkspaceStore((state) => state.setCommandOpen)
  const { activeCount } = useGlobalLens()
  const surface = SURFACE_BY_ID[activeSurface]

  return (
    <div className="pointer-events-auto fixed bottom-5 left-1/2 z-[35] hidden -translate-x-1/2 items-center gap-1 rounded-2xl border border-white/9 bg-[#0b0f13]/78 p-1 shadow-2xl shadow-black/20 backdrop-blur-xl md:flex">
      <div className="flex items-center gap-2 px-3 py-2">
        <span className="grid size-5 place-items-center rounded-md bg-white text-[9px] font-bold text-black">
          {activeSurface}
        </span>
        <div className="min-w-0">
          <p className="max-w-48 truncate text-[10px] font-medium text-white/74">
            {surface.name}
          </p>
          <p className="text-[9px] text-white/30">
            Issue #{surface.ownerIssue}
          </p>
        </div>
      </div>

      <span className="h-6 w-px bg-white/8" />

      <ContextMetric
        icon={Layers3}
        label="Lens"
        value={activeCount.toString()}
      />
      <ContextMetric label="Selected" value={selection.length.toString()} />
      <ContextMetric label="Compare" value={compare.length.toString()} />

      <button
        type="button"
        onClick={() => setCommandOpen(true)}
        className="ml-1 flex items-center gap-2 rounded-xl border border-white/8 bg-white/[0.035] px-3 py-2 text-[10px] text-white/48 transition hover:bg-white/[0.065] hover:text-white"
      >
        <Command className="size-3.5" aria-hidden />
        Command
        <span className="flex items-center gap-0.5 font-mono text-[9px] text-white/28">
          <CornerDownLeft className="size-2.5" />
          K
        </span>
      </button>
    </div>
  )
}

function ContextMetric({
  icon: Icon,
  label,
  value,
}: {
  icon?: typeof Layers3
  label: string
  value: string
}) {
  return (
    <div className="flex items-center gap-1.5 px-2 py-2 text-[9px] uppercase tracking-[0.12em] text-white/30">
      {Icon ? <Icon className="size-3" aria-hidden /> : null}
      {label}
      <span className="font-mono text-white/66">{value}</span>
    </div>
  )
}
