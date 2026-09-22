import { beforeEach, describe, expect, it } from "vitest"

import type { EntityRef } from "@/lib/intelligence/types"

import { useWorkspaceStore } from "./workspace-store"

const COMPANY: EntityRef = {
  id: "0100000000000",
  kind: "company",
  label: "Example Company",
}

function resetWorkspace() {
  useWorkspaceStore.setState({
    activeSurface: "M",
    focus: null,
    selection: [],
    compare: [],
    commandOpen: false,
    lensOpen: false,
    surfaceSnapshots: {},
    historyPast: [],
    historyFuture: [],
  })
}

describe("workspace history invariants", () => {
  beforeEach(resetWorkspace)

  it("preserves redo history when focus is already unchanged", () => {
    useWorkspaceStore.getState().setFocus(COMPANY)
    useWorkspaceStore.getState().undoContext()

    expect(useWorkspaceStore.getState().focus).toBeNull()
    expect(useWorkspaceStore.getState().historyFuture).toHaveLength(1)

    useWorkspaceStore.getState().setFocus(null)

    expect(useWorkspaceStore.getState().historyPast).toHaveLength(0)
    expect(useWorkspaceStore.getState().historyFuture).toHaveLength(1)

    useWorkspaceStore.getState().redoContext()
    expect(useWorkspaceStore.getState().focus).toEqual(COMPANY)
  })

  it("never leaves the lens layered over an opened command center", () => {
    useWorkspaceStore.getState().setLensOpen(true)
    useWorkspaceStore.getState().setCommandOpen(true)

    expect(useWorkspaceStore.getState().commandOpen).toBe(true)
    expect(useWorkspaceStore.getState().lensOpen).toBe(false)
  })
})
