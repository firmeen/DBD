"use client"

import * as React from "react"

import { isSurfaceId } from "@/lib/intelligence/surfaces"
import { useWorkspaceStore } from "@/stores/workspace-store"

function isEditableTarget(target: EventTarget | null) {
  if (!(target instanceof HTMLElement)) return false

  return (
    target.isContentEditable ||
    target.matches("input, textarea, select, [role='textbox'], [data-keyboard-input]")
  )
}

export function useKeyboardRouter() {
  const setActiveSurface = useWorkspaceStore((state) => state.setActiveSurface)
  const setCommandOpen = useWorkspaceStore((state) => state.setCommandOpen)
  const setLensOpen = useWorkspaceStore((state) => state.setLensOpen)

  React.useEffect(() => {
    function onKeyDown(event: KeyboardEvent) {
      if (event.defaultPrevented || event.repeat) return

      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault()
        setCommandOpen(true)
        return
      }

      if (isEditableTarget(event.target)) return
      if (event.metaKey || event.ctrlKey || event.altKey) return

      const key = event.key.toUpperCase()

      if (isSurfaceId(key)) {
        event.preventDefault()
        setActiveSurface(key)
        return
      }

      if (event.key === "?") {
        event.preventDefault()
        setCommandOpen(true)
        return
      }

      if (event.key.toLowerCase() === "f") {
        event.preventDefault()
        setLensOpen(true)
      }
    }

    window.addEventListener("keydown", onKeyDown)
    return () => window.removeEventListener("keydown", onKeyDown)
  }, [setActiveSurface, setCommandOpen, setLensOpen])
}
