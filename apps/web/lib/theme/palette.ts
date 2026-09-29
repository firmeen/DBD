export type TipxResolvedTheme = "light" | "dark"

export interface TipxRenderPalette {
  chart: {
    axis: string
    grid: string
    label: string
    muted: string
    tooltipBackground: string
    tooltipBorder: string
    tooltipText: string
    primary: string
    secondary: string
    accent: string
    selected: string
  }
  graph: {
    canvas: string
    node: string
    nodeMuted: string
    nodeCompany: string
    nodePerson: string
    edge: string
    edgeMuted: string
    selected: string
    path: string
    label: string
  }
  deck: {
    administrativeFill: [number, number, number, number]
    administrativeLine: [number, number, number, number]
    density: [number, number, number]
    densityLine: [number, number, number, number]
    opportunityLow: [number, number, number]
    opportunityHigh: [number, number, number]
    catchmentFill: [number, number, number, number]
    catchmentLine: [number, number, number, number]
    selected: [number, number, number, number]
    clusterFill: [number, number, number, number]
    clusterLine: [number, number, number, number]
    clusterText: [number, number, number, number]
    branchPrimary: [number, number, number, number]
    branchSecondary: [number, number, number, number]
    branchLine: [number, number, number, number]
    label: [number, number, number, number]
    labelBackground: [number, number, number, number]
  }
  entity: {
    active: string
    expired: string
    prospect: string
    border: string
    hover: string
    selected: string
    text: string
  }
}

export const TIPX_RENDER_PALETTES: Record<
  TipxResolvedTheme,
  TipxRenderPalette
> = {
  light: {
    chart: {
      axis: "rgba(30,42,87,.30)",
      grid: "rgba(30,42,87,.09)",
      label: "rgba(22,30,62,.64)",
      muted: "rgba(22,30,62,.42)",
      tooltipBackground: "rgba(255,255,255,.98)",
      tooltipBorder: "rgba(36,55,160,.16)",
      tooltipText: "rgba(20,28,58,.86)",
      primary: "#1f32b5",
      secondary: "#6878d7",
      accent: "#e4312b",
      selected: "#1730b9",
    },
    graph: {
      canvas: "#f4f6fc",
      node: "#51608d",
      nodeMuted: "#c7cce0",
      nodeCompany: "#2037b8",
      nodePerson: "#7d6bc7",
      edge: "#9aa5c7",
      edgeMuted: "#d9ddea",
      selected: "#1730b9",
      path: "#e4312b",
      label: "#1a2346",
    },
    deck: {
      administrativeFill: [46, 69, 180, 24],
      administrativeLine: [46, 69, 180, 76],
      density: [35, 74, 196],
      densityLine: [31, 45, 92, 96],
      opportunityLow: [255, 198, 78],
      opportunityHigh: [228, 49, 43],
      catchmentFill: [31, 50, 181, 24],
      catchmentLine: [31, 50, 181, 170],
      selected: [228, 49, 43, 230],
      clusterFill: [255, 255, 255, 238],
      clusterLine: [31, 50, 181, 168],
      clusterText: [25, 33, 73, 230],
      branchPrimary: [31, 50, 181, 245],
      branchSecondary: [105, 119, 172, 220],
      branchLine: [255, 255, 255, 235],
      label: [25, 33, 73, 220],
      labelBackground: [255, 255, 255, 230],
    },
    entity: {
      active: "#1f54a8",
      expired: "#c74740",
      prospect: "#69728c",
      border: "#b9c3dd",
      hover: "#1f32b5",
      selected: "#e4312b",
      text: "#ffffff",
    },
  },
  dark: {
    chart: {
      axis: "rgba(233,238,255,.28)",
      grid: "rgba(233,238,255,.07)",
      label: "rgba(239,242,255,.66)",
      muted: "rgba(239,242,255,.42)",
      tooltipBackground: "rgba(10,15,36,.98)",
      tooltipBorder: "rgba(139,154,255,.18)",
      tooltipText: "rgba(246,248,255,.86)",
      primary: "#8292ff",
      secondary: "#a8b2ff",
      accent: "#ff5a50",
      selected: "#a7b1ff",
    },
    graph: {
      canvas: "#090d1d",
      node: "#7180a6",
      nodeMuted: "#29324d",
      nodeCompany: "#7f90ff",
      nodePerson: "#a596e8",
      edge: "#394668",
      edgeMuted: "#1e263e",
      selected: "#a7b1ff",
      path: "#ff655c",
      label: "#f0f3ff",
    },
    deck: {
      administrativeFill: [96, 116, 196, 32],
      administrativeLine: [194, 205, 255, 72],
      density: [90, 126, 255],
      densityLine: [11, 16, 38, 128],
      opportunityLow: [234, 177, 57],
      opportunityHigh: [255, 90, 80],
      catchmentFill: [126, 146, 255, 28],
      catchmentLine: [151, 166, 255, 188],
      selected: [255, 90, 80, 235],
      clusterFill: [18, 25, 52, 230],
      clusterLine: [196, 205, 255, 160],
      clusterText: [247, 249, 255, 230],
      branchPrimary: [235, 239, 255, 245],
      branchSecondary: [159, 171, 211, 214],
      branchLine: [7, 10, 24, 230],
      label: [247, 249, 255, 205],
      labelBackground: [8, 12, 29, 205],
    },
    entity: {
      active: "#397fb2",
      expired: "#a44f49",
      prospect: "#646f89",
      border: "#aab7d2",
      hover: "#ffffff",
      selected: "#ff655c",
      text: "#ffffff",
    },
  },
}
