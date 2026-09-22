"use client"

import { Command } from "cmdk"
import { Search } from "lucide-react"
import { AnimatePresence, motion, useReducedMotion } from "motion/react"

import {
  INTELLIGENCE_COMMANDS,
  type IntelligenceCommand,
} from "@/lib/intelligence/commands"
import { useGlobalLens } from "@/hooks/use-global-lens"
import { useWorkspaceStore } from "@/stores/workspace-store"

export function CommandCenter() {
  const reduceMotion = useReducedMotion()
  const open = useWorkspaceStore((state) => state.commandOpen)
  const setOpen = useWorkspaceStore((state) => state.setCommandOpen)
  const setActiveSurface = useWorkspaceStore((state) => state.setActiveSurface)
  const setLensOpen = useWorkspaceStore((state) => state.setLensOpen)
  const setFocus = useWorkspaceStore((state) => state.setFocus)
  const clearSelection = useWorkspaceStore((state) => state.clearSelection)
  const { clearLens } = useGlobalLens()

  async function run(command: IntelligenceCommand) {
    if (command.surface) {
      setActiveSurface(command.surface)
    } else {
      switch (command.id) {
        case "lens.toggle":
          setLensOpen(true)
          break
        case "lens.clear":
          await clearLens()
          break
        case "workspace.clear-focus":
          setFocus(null)
          break
        case "workspace.clear-selection":
          clearSelection()
          break
      }
    }

    setOpen(false)
  }

  return (
    <AnimatePresence>
      {open ? (
        <motion.div
          className="fixed inset-0 z-[90] flex items-start justify-center bg-black/52 px-4 pt-[13vh] backdrop-blur-sm"
          initial={reduceMotion ? false : { opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          onMouseDown={(event) => {
            if (event.currentTarget === event.target) setOpen(false)
          }}
        >
          <motion.div
            initial={
              reduceMotion
                ? false
                : { opacity: 0, y: -8, scale: 0.985 }
            }
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={
              reduceMotion
                ? { opacity: 0 }
                : { opacity: 0, y: -6, scale: 0.99 }
            }
            transition={{ duration: 0.18, ease: [0.2, 0.8, 0.2, 1] }}
            className="w-full max-w-2xl overflow-hidden rounded-2xl border border-white/12 bg-[#0b0f13]/96 shadow-[0_30px_90px_rgba(0,0,0,.55)]"
          >
            <Command
              label="TIPX Command Center"
              loop
              className="[&_[cmdk-group-heading]]:px-4 [&_[cmdk-group-heading]]:pb-2 [&_[cmdk-group-heading]]:pt-4 [&_[cmdk-group-heading]]:text-[10px] [&_[cmdk-group-heading]]:font-semibold [&_[cmdk-group-heading]]:uppercase [&_[cmdk-group-heading]]:tracking-[0.2em] [&_[cmdk-group-heading]]:text-white/32"
            >
              <div className="flex items-center gap-3 border-b border-white/8 px-4">
                <Search className="size-4 text-white/35" aria-hidden />
                <Command.Input
                  autoFocus
                  placeholder="Navigate, filter or control the workspace…"
                  className="h-14 w-full bg-transparent text-sm text-white outline-none placeholder:text-white/28"
                />
                <kbd className="rounded-md border border-white/10 bg-white/5 px-2 py-1 font-mono text-[10px] text-white/36">
                  ESC
                </kbd>
              </div>

              <Command.List className="max-h-[min(440px,58vh)] overflow-y-auto p-2">
                <Command.Empty className="px-4 py-10 text-center text-xs text-white/42">
                  No matching intelligence command.
                </Command.Empty>

                {(["Navigate", "Lens", "Workspace"] as const).map(
                  (category) => (
                    <Command.Group key={category} heading={category}>
                      {INTELLIGENCE_COMMANDS.filter(
                        (command) =>
                          command.category === category &&
                          command.id !== "command.open"
                      ).map((command) => (
                        <Command.Item
                          key={command.id}
                          value={`${command.label} ${command.description}`}
                          onSelect={() => void run(command)}
                          className="group flex cursor-default items-center gap-3 rounded-xl px-3 py-3 text-sm text-white/72 outline-none data-[selected=true]:bg-white/8 data-[selected=true]:text-white"
                        >
                          <div className="min-w-0 flex-1">
                            <p className="font-medium">{command.label}</p>
                            <p className="mt-0.5 truncate text-[11px] text-white/36">
                              {command.description}
                            </p>
                          </div>
                          {command.shortcut ? (
                            <kbd className="rounded-md border border-white/9 bg-black/25 px-2 py-1 font-mono text-[10px] text-white/35 group-data-[selected=true]:text-white/58">
                              {command.shortcut}
                            </kbd>
                          ) : null}
                        </Command.Item>
                      ))}
                    </Command.Group>
                  )
                )}
              </Command.List>
            </Command>
          </motion.div>
        </motion.div>
      ) : null}
    </AnimatePresence>
  )
}
