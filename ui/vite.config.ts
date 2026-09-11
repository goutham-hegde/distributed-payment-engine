import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

/**
 * The dev server mirrors what nginx does in the container, and that is the point of it.
 *
 * In Compose the browser talks to nginx on one origin and nginx fans out to three services plus
 * Prometheus (see ui/nginx.conf). In `npm run dev` there is no nginx, so these proxy rules stand in
 * for it - same prefixes, same rewrite, different implementation. If the two ever disagree, the
 * app works in development and 404s in the image, which is the worst way round to find out.
 *
 * The alternative - pointing the browser straight at :8081/:8082/:8083 in dev - would need CORS
 * enabled on three services for the sake of a dev convenience, and CORS configuration written for
 * development has a way of shipping.
 */
export default defineConfig({
  plugins: [react()],
  server: {
    // 8085, beside the container's 8084 (infra/docker-compose.yml) - not Vite's default 5173, which
    // another project on this machine pins its dashboard to. strictPort because Vite's fallback is
    // to move silently to the next free port, and a dev server that is not where the README says
    // is found by opening a different app.
    port: 8085,
    strictPort: true,
    proxy: {
      "/api/orchestrator": {
        target: "http://localhost:8081",
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/api\/orchestrator/, ""),
      },
      "/api/accounts": {
        target: "http://localhost:8082",
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/api\/accounts/, ""),
      },
      "/api/gateway": {
        target: "http://localhost:8083",
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/api\/gateway/, ""),
      },
      "/api/prom": {
        target: "http://localhost:9090",
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/api\/prom/, ""),
      },
    },
  },
});
