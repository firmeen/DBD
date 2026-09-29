"use client"

import * as React from "react"
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { NuqsAdapter } from "nuqs/adapters/next/app"

import { ThemeProvider } from "@/components/theme-provider"

export function AppProviders({ children }: { children: React.ReactNode }) {
  const [queryClient] = React.useState(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: {
            staleTime: 30_000,
            refetchOnWindowFocus: false,
          },
        },
      })
  )

  return (
    <QueryClientProvider client={queryClient}>
      <NuqsAdapter>
        <ThemeProvider defaultTheme="system" enableSystem>
          {children}
        </ThemeProvider>
      </NuqsAdapter>
    </QueryClientProvider>
  )
}
