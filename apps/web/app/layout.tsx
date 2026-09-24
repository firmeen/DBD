import type { Metadata } from "next"
import { Geist_Mono, Noto_Sans_Thai } from "next/font/google"

import "./globals.css"
import "maplibre-gl/dist/maplibre-gl.css"
import { AppProviders } from "@/components/providers"
import { cn } from "@/lib/utils"

const sans = Noto_Sans_Thai({
  subsets: ["thai", "latin"],
  weight: ["400", "500", "600", "700"],
  variable: "--font-tipx-sans",
})

const mono = Geist_Mono({
  subsets: ["latin"],
  variable: "--font-tipx-mono",
})

export const metadata: Metadata = {
  title: {
    default: "TIPX Intelligence",
    template: "%s · TIPX",
  },
  description:
    "Cross-surface business intelligence workspace for company, people, spatial, relationship and analytical investigation.",
}

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode
}>) {
  return (
    <html
      lang="en"
      suppressHydrationWarning
      className={cn("antialiased", sans.variable, mono.variable)}
    >
      <body>
        <AppProviders>{children}</AppProviders>
      </body>
    </html>
  )
}
