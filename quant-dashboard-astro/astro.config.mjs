import fs from "fs";
import path from "path";
import zlib from "zlib";
import { fileURLToPath } from "url";
import { defineConfig } from "astro/config";
import react from "@astrojs/react";
import tailwind from "@astrojs/tailwind";
import viteCompression from "vite-plugin-compression";

function walkFiles(dir, onFile) {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) walkFiles(fullPath, onFile);
    else onFile(fullPath);
  }
}

const COMPRESS_EXTENSIONS = new Set([".html", ".css", ".svg", ".txt", ".xml"]);

function assetCompressionIntegration() {
  return {
    name: "asset-compression",
    hooks: {
      "astro:build:done": ({ dir }) => {
        const distDir = fileURLToPath(dir);
        if (!fs.existsSync(distDir)) return;

        let compressedCount = 0;

        walkFiles(distDir, (filePath) => {
          const ext = path.extname(filePath).toLowerCase();
          if (!COMPRESS_EXTENSIONS.has(ext)) return;

          const content = fs.readFileSync(filePath);
          fs.writeFileSync(
            `${filePath}.gz`,
            zlib.gzipSync(content, { level: 9 }),
          );
          fs.writeFileSync(
            `${filePath}.br`,
            zlib.brotliCompressSync(content, {
              params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 11 },
            }),
          );

          compressedCount += 1;
        });

        console.log(
          `[asset-compression] generated .gz/.br for ${compressedCount} files (${Array.from(COMPRESS_EXTENSIONS).join(", ")})`,
        );
      },
    },
  };
}

export default defineConfig({
  integrations: [react(), tailwind(), assetCompressionIntegration()],
  output: "static",
  build: {
    format: "directory",
  },
  vite: {
    plugins: [
      // viteCompression({
      //   algorithm: 'gzip',
      //   ext: '.gz',
      //   deleteOriginFile: false,
      // }),
      // viteCompression({
      //   algorithm: 'brotliCompress',
      //   ext: '.br',
      //   deleteOriginFile: false,
      // }),
    ],
    ssr: {
      noExternal: ["lightweight-charts"],
    },
  },
});
