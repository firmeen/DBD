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
  history: WorkspaceCheckpoint[]
  historyCursor: number
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

export const useWorkspaceStore = create<WorkspaceState>((set, get) => ({
  activeSurface: "M",
  focus: null,
  selection: [],
  compare: [],
  commandOpen: false,
  lensOpen: false,
  surfaceSnapshots: {},
  history: [],
  historyCursor: -1,

  setActiveSurface: (activeSurface) => {
    get().checkpoint()
    set({ activeSurface })
  },

  setFocus: (focus) => set({ focus }),

  setSelection: (selection) => set({ selection }),

  toggleSelection: (entity) =>
    set((state) => ({
      selection: state.selection.some((candidate) =>
        sameEntity(candidate, entity)
      )
        ? state.selection.filter((candidate) => !sameEntity(candidate, entity))
        : [...state.selection, entity],
    })),

  clearSelection: () => set({ selection: [] }),

  toggleCompare: (entity) =>
    set((state) => ({
      compare: state.compare.some((candidate) => sameEntity(candidate, entity))
        ? state.compare.filter((candidate) => !sameEntity(candidate, entity))
        : [...state.compare, entity],
    })),

  clearCompare: () => set({ compare: [] }),

  setCommandOpen: (commandOpen) => set({ commandOpen }),

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

  checkpoint: () => {
    const state = get()
    const snapshot: WorkspaceCheckpoint = {
      surface: state.activeSurface,
      focus: state.focus,
      selection: state.selection,
      compare: state.compare,
    }
    const kept = state.history.slice(0, state.historyCursor + 1)
    const history = [...kept, snapshot].slice(-MAX_HISTORY)
    set({
      history,
      historyCursor: history.length - 1,
    })
  },

  undoContext: () => {
    const state = get()
    if (state.historyCursor < 0) return
    const snapshot = state.history[state.historyCursor]
    if (!snapshot) return

    set({
      activeSurface: snapshot.surface,
      focus: snapshot.focus,
      selection: snapshot.selection,
      compare: snapshot.compare,
      historyCursor: state.historyCursor - 1,
    })
  },

  redoContext: () => {
    const state = get()
    const nextCursor = state.historyCursor + 1
    const snapshot = state.history[nextCursor]
    if (!snapshot) return

    set({
      activeSurface: snapshot.surface,
      focus: snapshot.focus,
      selection: snapshot.selection,
      compare: snapshot.compare,
      historyCursor: nextCursor,
    })
  },
}))
