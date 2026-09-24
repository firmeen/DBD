import { copyFile, mkdir } from "node:fs/promises"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"

const root = dirname(dirname(fileURLToPath(import.meta.url)))
const source = join(root, "node_modules", "maplibre-gl", "dist")
const destination = join(root, "public", "maplibre")

await mkdir(destination, { recursive: true })

for (const filename of [
  "maplibre-gl-worker.mjs",
  "maplibre-gl-shared.mjs",
]) {
  await copyFile(join(source, filename), join(destination, filename))
}

console.log("Prepared MapLibre worker assets in public/maplibre")
