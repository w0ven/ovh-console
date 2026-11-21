import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react-swc";
import { TanStackRouterVite } from "@tanstack/router-plugin/vite";
import path from "path";

/**
 * Vite 配置：
 * - dev server 监听 19997，开发期把 /api 反代到 Go backend (19998)
 * - TanStack Router 文件路由插件自动生成 routeTree.gen.ts
 * - @ 路径别名指向 src
 */
export default defineConfig(({ mode }) => {
  // loadEnv 会把 .env.development.local(gitignore)里的变量读进来;
  // vite.config 的 process.env 不自动加载 env 文件,必须显式 loadEnv
  const env = loadEnv(mode, __dirname, "");
  return {
  server: {
    port: 19997,
    host: true,
    proxy: {
      "/api": {
        // 开发代理目标可被 VITE_API_PROXY_TARGET 覆盖(值放 .env.development.local,gitignore,不进仓库)
        target: env.VITE_API_PROXY_TARGET || "http://localhost:19998",
        changeOrigin: true,
      },
    },
  },
  build: {
    // 直接输出到 server/web，配合 server/webembed_ui.go 的 //go:embed
    // 让 `go build -tags ui` 自动把整个前端打进二进制
    outDir: path.resolve(__dirname, "../server/web"),
    emptyOutDir: true,
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (id.includes("/src/i18n/locales/")) {
            if (id.endsWith(".zh.ts") || id.endsWith("/zh.ts") || id.includes("api-zh.")) {
              return "locale-zh";
            }
            if (id.endsWith(".en.ts") || id.endsWith("/en.ts") || id.includes("api-en.")) {
              return "locale-en";
            }
          }
          if (!id.includes("node_modules")) {
            return undefined;
          }
          if (id.includes("/recharts/") || id.includes("/d3-") || id.includes("/victory-vendor/")) {
            return "vendor-charts";
          }
          if (id.includes("/framer-motion/") || id.includes("/motion-dom/") || id.includes("/motion-utils/")) {
            return "vendor-motion";
          }
          if (id.includes("/@tanstack/")) {
            return "vendor-tanstack";
          }
          if (id.includes("/@radix-ui/") || id.includes("/cmdk/") || id.includes("/vaul/")) {
            return "vendor-ui";
          }
          if (id.includes("/i18next") || id.includes("/react-i18next/")) {
            return "vendor-i18n";
          }
          if (id.includes("/lucide-react/")) {
            return "vendor-icons";
          }
          if (id.includes("/date-fns/")) {
            return "vendor-date";
          }
          if (
            id.includes("/node_modules/react/") ||
            id.includes("/node_modules/react-dom/") ||
            id.includes("/node_modules/scheduler/")
          ) {
            return "vendor-react";
          }
          return undefined;
        },
      },
    },
  },
  plugins: [
    TanStackRouterVite({
      routesDirectory: "./src/routes",
      generatedRouteTree: "./src/routeTree.gen.ts",
      autoCodeSplitting: true,
    }),
    react(),
  ],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  };
});
