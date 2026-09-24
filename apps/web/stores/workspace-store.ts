"use client"

import { create } from "zustand"

import type {
  EntityRef,
  PopulationRef,
  SurfaceId,
  SurfaceSnapshot,
} from "@/lib/intelligence/types"

const MAX_HISTORY = 24

export interface WorkspaceCheckpoint {
  surface: SurfaceId
  focus: EntityRef | null
  selection: EntityRef[]
  compare: EntityRef[]
  population: PopulationRef | null
}

interface WorkspaceState {
  activeSurface: SurfaceId
  focus: EntityRef | null
  selection: EntityRef[]
  compare: EntityRef[]
  population: PopulationRef | null
  commandOpen: boolean
  lensOpen: boolean
  surfaceSnapshots: Partial<Record<SurfaceId, SurfaceSnapshot>>
  historyPast: WorkspaceCheckpoint[]
  historyFuture: WorkspaceCheckpoint[]
  setActiveSurface: (surface: SurfaceId) => void
  setFocus: (entity: EntityRef | null) => void
  setSelection: (entities: EntityRef[]) => void
  toggleSelection: (entity: EntityRef) => void
  clearSelection: () => void
  toggleCompare: (entity: EntityRef) => void
  clearCompare: () => void
  setPopulation: (population: PopulationRef | null) => void
  setCommandOpen: (open: boolean) => void
  setLensOpen: (open: boolean) => void
  saveSurfaceSnapshot: (
    surface: SurfaceId,
    payload: Record<string, unknown>
  ) => void
  checkpoint: () => void
  undoContext: () => void
  redoContext: () => void
}

function sameEntity(left: EntityRef, right: EntityRef) {
  return left.kind === right.kind && left.id === right.id
}

function sameEntityValue(left: EntityRef, right: EntityRef) {
  return (
    sameEntity(left, right) &&
    left.label === right.label &&
    left.secondary === right.secondary
  )
}

function sameEntityList(left: EntityRef[], right: EntityRef[]) {
  return (
    left.length === right.length &&
    left.every((entity, index) => sameEntityValue(entity, right[index]))
  )
}

function snapshotOf(state: WorkspaceState): WorkspaceCheckpoint {
  return {
    surface: state.activeSurface,
    focus: state.focus,
    selection: state.selection,
    compare: state.compare,
    population: state.population,
  }
}

function recordBeforeMutation(state: WorkspaceState) {
  return {
    historyPast: [...state.historyPast, snapshotOf(state)].slice(-MAX_HISTORY),
    historyFuture: [] as WorkspaceCheckpoint[],
  }
}

export const useWorkspaceStore = create<WorkspaceState>((set, get) => ({
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

  setActiveSurface: (activeSurface) =>
    set((state) =>
      state.activeSurface === activeSurface
        ? state
        : {
            ...recordBeforeMutation(state),
            activeSurface,
          }
    ),

  setFocus: (focus) =>
    set((state) => {
      const unchanged =
        state.focus === focus ||
        (state.focus !== null &&
          focus !== null &&
          sameEntityValue(state.focus, focus))

      return unchanged
        ? state
        : {
            ...recordBeforeMutation(state),
            focus,
          }
    }),

  setSelection: (selection) =>
    set((state) =>
      sameEntityList(state.selection, selection)
        ? state
        : {
            ...recordBeforeMutation(state),
            selection,
          }
    ),

  toggleSelection: (entity) =>
    set((state) => ({
      ...recordBeforeMutation(state),
      selection: state.selection.some((candidate) =>
        sameEntity(candidate, entity)
      )
        ? state.selection.filter((candidate) => !sameEntity(candidate, entity))
        : [...state.selection, entity],
    })),

  clearSelection: () =>
    set((state) =>
      state.selection.length === 0
        ? state
        : {
            ...recordBeforeMutation(state),
            selection: [],
          }
    ),

  toggleCompare: (entity) =>
    set((state) => ({
      ...recordBeforeMutation(state),
      compare: state.compare.some((candidate) => sameEntity(candidate, entity))
        ? state.compare.filter((candidate) => !sameEntity(candidate, entity))
        : [...state.compare, entity],
    })),

  clearCompare: () =>
    set((state) =>
      state.compare.length === 0
        ? state
        : {
            ...recordBeforeMutation(state),
            compare: [],
          }
    ),

  setPopulation: (population) =>
    set((state) => {
      const unchanged =
        state.population === population ||
        (state.population !== null &&
          population !== null &&
          state.population.key === population.key &&
          state.population.label === population.label &&
          state.population.count === population.count &&
          state.population.source === population.source &&
          state.population.exact === population.exact &&
          state.population.expiresAt === population.expiresAt)

      return unchanged
        ? state
        : {
            ...recordBeforeMutation(state),
            population,
          }
    }),

  setCommandOpen: (commandOpen) =>
    set((state) =>
      commandOpen
        ? { commandOpen: true, lensOpen: false }
        : state.commandOpen
          ? { commandOpen: false }
          : state
    ),

  setLensOpen: (lensOpen) =>
    set((state) =>
      lensOpen
        ? { lensOpen: true, commandOpen: false }
        : state.lensOpen
          ? { lensOpen: false }
          : state
    ),

  saveSurfaceSnapshot: (surface, payload) =>
    set((state) => ({
      surfaceSnapshots: {
        ...state.surfaceSnapshots,
        [surface]: {
          updatedAt: Date.now(),
          payload,
        },
      },
    })),

  checkpoint: () =>
    set((state) => ({
      ...recordBeforeMutation(state),
    })),

  undoContext: () => {
    const state = get()
    const previous = state.historyPast.at(-1)
    if (!previous) return

    const current = snapshotOf(state)

    set({
      activeSurface: previous.surface,
      focus: previous.focus,
      selection: previous.selection,
      compare: previous.compare,
      population: previous.population,
      historyPast: state.historyPast.slice(0, -1),
      historyFuture: [current, ...state.historyFuture].slice(0, MAX_HISTORY),
    })
  },

  redoContext: () => {
    const state = get()
    const next = state.historyFuture[0]
    if (!next) return

    const current = snapshotOf(state)

    set({
      activeSurface: next.surface,
      focus: next.focus,
      selection: next.selection,
      compare: next.compare,
      population: next.population,
      historyPast: [...state.historyPast, current].slice(-MAX_HISTORY),
      historyFuture: state.historyFuture.slice(1),
    })
  },
}))
