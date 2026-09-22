"use client"

import { create } from "zustand"

import type {
  EntityRef,
  SurfaceId,
  SurfaceSnapshot,
} from "@/lib/intelligence/types"

const MAX_HISTORY = 24

export interface WorkspaceCheckpoint {
  surface: SurfaceId
  focus: EntityRef | null
  selection: EntityRef[]
  compare: EntityRef[]
}

interface WorkspaceState {
  activeSurface: SurfaceId
  focus: EntityRef | null
  selection: EntityRef[]
  compare: EntityRef[]
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

function snapshotOf(state: WorkspaceState): WorkspaceCheckpoint {
  return {
    surface: state.activeSurface,
    focus: state.focus,
    selection: state.selection,
    compare: state.compare,
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
          state.focus.kind === focus.kind &&
          state.focus.id === focus.id &&
          state.focus.label === focus.label &&
          state.focus.secondary === focus.secondary)

      return unchanged
        ? state
        : {
            ...recordBeforeMutation(state),
            focus,
          }
    }),

  setSelection: (selection) =>
    set((state) => ({
      ...recordBeforeMutation(state),
      selection,
    })),

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

  setCommandOpen: (commandOpen) =>
    set((state) =>
      commandOpen
        ? { commandOpen: true, lensOpen: false }
        : state.commandOpen
          ? { commandOpen: false }
          : state
    ),

  setLensOpen: (lensOpen) => set({ lensOpen }),

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
      historyPast: [...state.historyPast, current].slice(-MAX_HISTORY),
      historyFuture: state.historyFuture.slice(1),
    })
  },
}))
