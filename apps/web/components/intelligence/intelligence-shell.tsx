"use client"

import { CommandCenter } from "./command-center"
import { CompareTray } from "./compare-tray"
import { ContextBar } from "./context-bar"
import { EntityDock } from "./entity-dock"
import { FloatingOrbit } from "./floating-orbit"
import { GlobalLens } from "./global-lens"
import { SurfaceStage } from "./surface-stage"
import { useKeyboardRouter } from "@/hooks/use-keyboard-router"

export function IntelligenceShell() {
  useKeyboardRouter()

  return (
    <div className="relative min-h-svh bg-tipx-canvas">
      <SurfaceStage />
      <FloatingOrbit />
      <GlobalLens />
      <ContextBar />
      <EntityDock />
      <CompareTray />
      <CommandCenter />
    </div>
  )
}
