import { assign, createMachine } from "xstate"

import type { SurfaceId } from "./types"

type OrbitContext = {
  peekedSurface: SurfaceId | null
}

type OrbitEvent =
  | { type: "POINTER_ENTER" }
  | { type: "POINTER_LEAVE" }
  | { type: "PEEK"; surface: SurfaceId }
  | { type: "CLEAR_PEEK" }
  | { type: "OPEN_COMMANDS" }
  | { type: "CLOSE_COMMANDS" }

export const orbitMachine = createMachine({
  types: {} as {
    context: OrbitContext
    events: OrbitEvent
  },
  context: {
    peekedSurface: null,
  },
  initial: "resting",
  states: {
    resting: {
      on: {
        POINTER_ENTER: "awake",
        OPEN_COMMANDS: "commanding",
      },
    },
    awake: {
      on: {
        PEEK: {
          actions: assign({
            peekedSurface: ({ event }) =>
              event.type === "PEEK" ? event.surface : null,
          }),
        },
        CLEAR_PEEK: {
          actions: assign({ peekedSurface: () => null }),
        },
        POINTER_LEAVE: {
          target: "resting",
          actions: assign({ peekedSurface: () => null }),
        },
        OPEN_COMMANDS: "commanding",
      },
    },
    commanding: {
      on: {
        CLOSE_COMMANDS: "awake",
      },
    },
  },
})
