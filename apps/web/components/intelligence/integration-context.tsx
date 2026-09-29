"use client"

import * as React from "react"

import type { IntegrationLaunchContext } from "@/lib/integration/types"

const TipxIntegrationContext =
  React.createContext<IntegrationLaunchContext | null>(null)

export function TipxIntegrationContextProvider({
  value,
  children,
}: {
  value: IntegrationLaunchContext | null
  children: React.ReactNode
}) {
  return (
    <TipxIntegrationContext.Provider value={value}>
      {children}
    </TipxIntegrationContext.Provider>
  )
}

export function useTipxIntegrationContext() {
  return React.useContext(TipxIntegrationContext)
}
