"use client"

import * as React from "react"

import { TipxIntegrationContextProvider } from "./integration-context"
import type { EntityRef } from "@/lib/intelligence/types"
import type { IntegrationLaunchContext } from "@/lib/integration/types"
import { useWorkspaceStore } from "@/stores/workspace-store"

export function IntegrationBootstrap({
  context,
  children,
}: {
  context: IntegrationLaunchContext | null
  children: React.ReactNode
}) {
  const hydrateExternalContext = useWorkspaceStore(
    (state) => state.hydrateExternalContext
  )
  const [ready, setReady] = React.useState(context === null)

  React.useEffect(() => {
    if (!context) {
      setReady(true)
      return
    }

    const entity = context.target.entity
    const focus: EntityRef | null = entity
      ? {
          id: entity.id,
          kind: entity.kind,
          label: entity.labelHint ?? entity.id,
        }
      : null

    hydrateExternalContext({
      activeSurface: context.target.surface,
      focus,
    })
    setReady(true)
  }, [context, hydrateExternalContext])

  if (!ready) {
    return <IntegrationLaunchLoader sourceApp={context?.sourceApp ?? null} />
  }

  return (
    <TipxIntegrationContextProvider value={context}>
      {children}
    </TipxIntegrationContextProvider>
  )
}

function IntegrationLaunchLoader({
  sourceApp,
}: {
  sourceApp: string | null
}) {
  return (
    <main className="tipx-brand-field relative grid min-h-svh place-items-center overflow-hidden bg-tipx-canvas text-tipx-ink">
      <div className="tipx-grid absolute inset-0 opacity-30" />
      <div className="tipx-vignette absolute inset-0" />
      <div className="relative z-10 flex items-center gap-3 rounded-2xl border border-tipx-line/8 bg-tipx-floating/90 px-4 py-3 text-[10px] font-semibold uppercase tracking-[0.2em] text-tipx-ink/42 shadow-2xl backdrop-blur-xl">
        <span className="size-1.5 animate-pulse rounded-full bg-tipx-brand/70" />
        Opening TIPX context
        {sourceApp ? (
          <span className="font-mono text-tipx-ink/24">· {sourceApp}</span>
        ) : null}
      </div>
    </main>
  )
}
