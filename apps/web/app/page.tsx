import { Suspense } from "react"

import { IntelligenceShell } from "@/components/intelligence/intelligence-shell"
import { parseTipxIntegrationSearchParams } from "@/lib/integration/launch-url"

type PageSearchParams = Promise<
  Record<string, string | string[] | undefined>
>

export default async function Page({
  searchParams,
}: {
  searchParams: PageSearchParams
}) {
  const integrationContext = parseTipxIntegrationSearchParams(
    await searchParams
  )

  return (
    <Suspense fallback={<IntelligenceShellFallback />}>
      <IntelligenceShell integrationContext={integrationContext} />
    </Suspense>
  )
}

function IntelligenceShellFallback() {
  return (
    <main className="relative grid min-h-svh place-items-center overflow-hidden bg-[#070a0d] text-white">
      <div className="tipx-grid absolute inset-0 opacity-30" />
      <div className="tipx-vignette absolute inset-0" />
      <div className="relative z-10 flex items-center gap-3 rounded-2xl border border-white/8 bg-white/[0.025] px-4 py-3 text-[10px] font-semibold uppercase tracking-[0.2em] text-white/38 backdrop-blur-xl">
        <span className="size-1.5 animate-pulse rounded-full bg-white/60" />
        Initializing intelligence context
      </div>
    </main>
  )
}
