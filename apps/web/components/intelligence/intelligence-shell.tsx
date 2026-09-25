"use client"

import { CommandCenter } from "./command-center"
import { CompareTray } from "./compare-tray"
import { ContextBar } from "./context-bar"
import { EntityDock } from "./entity-dock"
import { FloatingOrbit } from "./floating-orbit"
import { GlobalLens } from "./global-lens"
import { IntegrationBootstrap } from "./integration-bootstrap"
import { SurfaceStage } from "./surface-stage"
import { useKeyboardRouter } from "@/hooks/use-keyboard-router"
import type { IntegrationLaunchContext } from "@/lib/integration/types"

export function IntelligenceShell({
  integrationContext = null,
}: {
  integrationContext?: IntegrationLaunchContext | null
}) {
  useKeyboardRouter()

  return (
    <IntegrationBootstrap context={integrationContext}>
      <div className="relative min-h-svh bg-tipx-canvas">
        <SurfaceStage />
        <FloatingOrbit />
        <GlobalLens />
        <ContextBar />
        <EntityDock />
        <CompareTray />
        <CommandCenter />
      </div>
    </IntegrationBootstrap>
  )
}
