import { createRequire } from "node:module"

import type { NextConfig } from "next"

const require = createRequire(import.meta.url)

const lumaSingletonAliases = Object.fromEntries(
  [
    "@luma.gl/core",
    "@luma.gl/engine",
    "@luma.gl/shadertools",
    "@luma.gl/webgl",
  ].map((packageName) => [
    `${packageName}$`,
    require.resolve(packageName),
  ])
)

const nextConfig: NextConfig = {
  webpack(config) {
    config.resolve.alias = {
      ...(config.resolve.alias ?? {}),
      ...lumaSingletonAliases,
    }

    return config
  },
}

export default nextConfig
