import { SURFACES } from "./surfaces"
import type { IntelligenceCommandId, SurfaceId } from "./types"

export type CommandCategory = "Navigate" | "Lens" | "Workspace"

export interface IntelligenceCommand {
  id: IntelligenceCommandId
  label: string
  description: string
  shortcut?: string
  category: CommandCategory
  surface?: SurfaceId
}

export const INTELLIGENCE_COMMANDS: readonly IntelligenceCommand[] = [
  ...SURFACES.map(
    (surface): IntelligenceCommand => ({
      id: `surface.${surface.id.toLowerCase()}` as IntelligenceCommandId,
      label: surface.name,
      description: surface.description,
      shortcut: surface.id,
      category: "Navigate",
      surface: surface.id,
    })
  ),
  {
    id: "lens.toggle",
    label: "Global Intelligence Lens",
    description: "Open or close the shared analytical-universe controls.",
    shortcut: "F",
    category: "Lens",
  },
  {
    id: "lens.clear",
    label: "Clear Global Lens",
    description: "Remove shareable Lens dimensions without changing entity context.",
    category: "Lens",
  },
  {
    id: "workspace.clear-focus",
    label: "Clear focused entity",
    description: "Release the entity currently under investigation.",
    category: "Workspace",
  },
  {
    id: "workspace.clear-selection",
    label: "Clear analytical selection",
    description: "Clear the active population while preserving focus and compare.",
    category: "Workspace",
  },
  {
    id: "command.open",
    label: "Command Center",
    description: "Search navigation and workspace commands.",
    shortcut: "Ctrl K",
    category: "Workspace",
  },
] as const
