import { beforeEach, describe, expect, it } from "vitest"

import type { EntityRef, PopulationRef } from "@/lib/intelligence/types"

import { useWorkspaceStore } from "./workspace-store"

const COMPANY: EntityRef = {
  id: "0100000000000",
  kind: "company",
  label: "Example Company",
}

const POPULATION: PopulationRef = {
  key: "population:bangkok-prospects",
  label: "Bangkok prospects",
  count: 8421,
  source: "analytics",
  exact: true,
}

function resetWorkspace() {
  useWorkspaceStore.setState({
    activeSurface: "M",
    focus: null,
    selection: [],
    compare: [],
    population: null,
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

  it("preserves redo history when selection is already unchanged", () => {
    useWorkspaceStore.getState().setFocus(COMPANY)
    useWorkspaceStore.getState().undoContext()

    expect(useWorkspaceStore.getState().historyFuture).toHaveLength(1)

    useWorkspaceStore.getState().setSelection([])

    expect(useWorkspaceStore.getState().historyPast).toHaveLength(0)
    expect(useWorkspaceStore.getState().historyFuture).toHaveLength(1)
  })

  it("includes analytical population in undo and redo context", () => {
    useWorkspaceStore.getState().setPopulation(POPULATION)

    expect(useWorkspaceStore.getState().population).toEqual(POPULATION)

    useWorkspaceStore.getState().undoContext()
    expect(useWorkspaceStore.getState().population).toBeNull()

    useWorkspaceStore.getState().redoContext()
    expect(useWorkspaceStore.getState().population).toEqual(POPULATION)
  })

  it("preserves redo history when analytical population is unchanged", () => {
    useWorkspaceStore.getState().setPopulation(POPULATION)
    useWorkspaceStore.getState().undoContext()

    expect(useWorkspaceStore.getState().historyFuture).toHaveLength(1)

    useWorkspaceStore.getState().setPopulation(null)

    expect(useWorkspaceStore.getState().historyPast).toHaveLength(0)
    expect(useWorkspaceStore.getState().historyFuture).toHaveLength(1)
  })

  it("keeps command center and Global Lens mutually exclusive", () => {
    useWorkspaceStore.getState().setLensOpen(true)
    useWorkspaceStore.getState().setCommandOpen(true)

    expect(useWorkspaceStore.getState().commandOpen).toBe(true)
    expect(useWorkspaceStore.getState().lensOpen).toBe(false)

    useWorkspaceStore.getState().setLensOpen(true)

    expect(useWorkspaceStore.getState().lensOpen).toBe(true)
    expect(useWorkspaceStore.getState().commandOpen).toBe(false)
  })
})
