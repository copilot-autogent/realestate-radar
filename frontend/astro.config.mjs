import { defineConfig } from "astro/config";

export default defineConfig({
  site: "https://copilot-autogent.github.io",
  base: "/realestate-radar",
  output: "static",
  vite: {
    server: {
      proxy: {
        "/api": {
          target: "http://localhost:3001",
          changeOrigin: true,
        },
      },
    },
  },
});
