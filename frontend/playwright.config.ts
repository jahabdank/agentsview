import { defineConfig } from "@playwright/test";

const isCI = process.env.CI === "true";
const e2ePort = Number(process.env.AGENTSVIEW_E2E_PORT ?? "8090");

export default defineConfig({
  testDir: "e2e",
  timeout: isCI ? 45_000 : 20_000,
  retries: 0,
  use: {
    baseURL: `http://127.0.0.1:${e2ePort}`,
    headless: true,
    // Wide enough that the kit-ui TopBar renders its expanded tab row (at
    // Playwright's 1280px default the eight tabs collapse into the nav
    // SelectDropdown), so navigation specs can drive the tabs directly.
    viewport: { width: 1600, height: 900 },
  },
  projects: [
    {
      name: "chromium",
      use: { browserName: "chromium" },
    },
    {
      name: "webkit",
      use: { browserName: "webkit" },
    },
  ],
  webServer: {
    command: "bash ../scripts/e2e-server.sh",
    port: e2ePort,
    reuseExistingServer: false,
    timeout: 30_000,
  },
});
