import { defineConfig } from "astro/config";
import react from "@astrojs/react";
import tailwind from "@astrojs/tailwind";
import compressor from "astro-compressor";

export default defineConfig({
  integrations: [
    react(),
    tailwind(),
    compressor({
      gzip: { level: 9 },
      brotli: true,
      zstd: false,
      fileExtensions: [
        ".html",
        ".css",
        ".svg",
        ".txt",
        ".xml",
        ".js",
        ".cjs",
        ".mjs",
        ".json",
      ],
    }),
  ],
  output: "static",
  build: {
    format: "directory",
  },
  vite: {
    ssr: {
      noExternal: ["lightweight-charts"],
    },
  },
});
