import { app, BrowserWindow, dialog, Menu, nativeImage, powerSaveBlocker, shell, Tray, ipcMain } from "electron";
import { realpathSync, statSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { BackendService, resolveDataDirectory } from "./service.mjs";
import { safeErrorMessage, validateRpcRequest } from "./protocol.mjs";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const APP_NAME = "YakShed";
const isPackaged = app.isPackaged;
const DEV_RENDERER_URL = isPackaged ? "" : validateDevRendererUrl(process.env.YAKSHED_RENDERER_URL || "");
const isDev = !isPackaged && (process.argv.includes("--dev") || Boolean(DEV_RENDERER_URL));
const demo = process.argv.includes("--demo");
const preview = process.argv.includes("--browser-preview");
const dataOverride = argumentValue("--data-dir");
const smoke = process.argv.includes("--smoke");

if (smoke && dataOverride) {
  app.setPath("userData", path.join(path.resolve(dataOverride), "electron-profile"));
}

let mainWindow;
let backend;
let tray;
let quitting = false;
let quitStarted = false;
let stayAwake = false;
let stayAwakePreference = false;
let stayAwakeBlocker;
let menubarEnabled = false;
let activeRunCount = 0;
let startupError;
let smokeBackendReady = false;
let smokeRendererReady = false;
let smokeTimer;
let smokeFinishTimer;
const delegatedLoginUrls = new Map();

if (!app.requestSingleInstanceLock()) {
  app.quit();
} else {
  app.on("second-instance", () => {
    if (!mainWindow) return;
    if (mainWindow.isMinimized()) mainWindow.restore();
    mainWindow.show();
    mainWindow.focus();
  });
  app.whenReady().then(start).catch((error) => {
    // Keep the failure visible through the renderer where possible; never print
    // a provider/runtime error that could contain delegated credentials.
    if (mainWindow && !mainWindow.isDestroyed()) {
      mainWindow.webContents.send("yakshed:event", { event: "backend_error", message: safeErrorMessage(error) });
    }
  });
}

function argumentValue(name) {
  const index = process.argv.indexOf(name);
  if (index === -1) return undefined;
  const value = process.argv[index + 1];
  return value && !value.startsWith("-") ? value : undefined;
}

function validateDevRendererUrl(value) {
  if (!value) return "";
  try {
    const url = new URL(value);
    if (url.protocol !== "http:" || !["127.0.0.1", "localhost"].includes(url.hostname) || url.username || url.password || url.search || url.hash) {
      throw new Error("development renderer must be loopback HTTP");
    }
    return url.href.replace(/\/$/, "");
  } catch {
    throw new Error("invalid YAKSHED_RENDERER_URL; only loopback HTTP is allowed");
  }
}

async function start() {
  app.setName(APP_NAME);
  app.setAppUserModelId("com.yakshed.app");
  registerIpc();
  backend = new BackendService({
    dataDir: resolveDataDirectory(app.getPath("userData"), { demo, override: dataOverride }),
    demo,
    packaged: isPackaged,
    serviceBinary: undefined,
  });
  backend.on("event", (event) => {
    if (event.event === "changed") updateActiveCount(event);
    sendEvent(event);
  });
  backend.on("error", (error) => sendEvent({ event: "backend_error", message: safeErrorMessage(error) }));
  try {
    backend.start();
  } catch (error) {
    startupError = safeErrorMessage(error);
  }
  createWindow();
  if (startupError) sendEvent({ event: "backend_error", message: startupError });
  if (preview) sendEvent({ event: "preview", enabled: true });
  if (smoke) startSmokeCheck();
}

function createWindow() {
  if (mainWindow && !mainWindow.isDestroyed()) {
    mainWindow.show();
    return mainWindow;
  }
  mainWindow = new BrowserWindow({
    width: 1_440,
    height: 960,
    minWidth: 960,
    minHeight: 640,
    title: APP_NAME,
    backgroundColor: "#111318",
    show: false,
    webPreferences: {
      preload: path.join(ROOT, "desktop", "preload.cjs"),
      contextIsolation: true,
      sandbox: true,
      nodeIntegration: false,
      webSecurity: true,
      spellcheck: true,
      webviewTag: false,
    },
  });
  installNavigationGuards(mainWindow);
  mainWindow.once("ready-to-show", () => mainWindow.show());
  mainWindow.on("close", (event) => {
    if (menubarEnabled && !quitting) {
      event.preventDefault();
      mainWindow.hide();
    }
  });
  mainWindow.on("closed", () => {
    mainWindow = undefined;
  });
  const rendererUrl = DEV_RENDERER_URL || (isDev ? "http://127.0.0.1:5173" : "");
  const load = rendererUrl
    ? mainWindow.loadURL(rendererUrl)
    : mainWindow.loadFile(path.join(app.getAppPath(), "desktop", "frontend", "index.html"));
  load.catch((error) => sendEvent({ event: "backend_error", message: safeErrorMessage(error, "YakShed renderer failed to load") }));
  if (backend?.lastError) sendEvent({ event: "backend_error", message: "YakShed service reported an error" });
  return mainWindow;
}

function installNavigationGuards(window) {
  window.webContents.setWindowOpenHandler(({ url }) => {
    if (consumeDelegatedLogin(url)) void shell.openExternal(url);
    return { action: "deny" };
  });
  window.webContents.on("will-navigate", (event, url) => {
    if (isLocalRendererUrl(url)) return;
    event.preventDefault();
    if (consumeDelegatedLogin(url)) void shell.openExternal(url);
  });
  const rendererSession = window.webContents.session;
  rendererSession.setPermissionRequestHandler((_webContents, _permission, callback) => callback(false));
  rendererSession.webRequest.onHeadersReceived({ urls: ["*://*/*", "file://*/*"] }, (details, callback) => {
    const connect = isDev ? "'self' http://127.0.0.1:* ws://127.0.0.1:*" : "'self'";
    callback({
      responseHeaders: {
        ...details.responseHeaders,
        "Content-Security-Policy": [`default-src 'self'; base-uri 'none'; object-src 'none'; frame-ancestors 'none'; form-action 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self' data:; connect-src ${connect}`],
      },
    });
  });
}

function isLocalRendererUrl(value) {
  if (DEV_RENDERER_URL) {
    try {
      const expected = new URL(DEV_RENDERER_URL);
      const actual = new URL(value);
      return actual.origin === expected.origin && actual.pathname === expected.pathname && !actual.search && !actual.hash;
    } catch {
      return false;
    }
  }
  try {
    const expectedPath = path.normalize(path.join(app.getAppPath(), "desktop", "frontend", "index.html"));
    const actual = new URL(value);
    return actual.protocol === "file:" && decodeURIComponent(actual.pathname) === expectedPath && !actual.search && !actual.hash;
  } catch {
    return false;
  }
}

function registerIpc() {
  ipcMain.removeHandler("yakshed:request");
  ipcMain.removeHandler("yakshed:choose-workspace");
  ipcMain.removeAllListeners("yakshed:renderer-ready");
  ipcMain.on("yakshed:renderer-ready", (event) => {
    assertTrustedRenderer(event);
    smokeRendererReady = true;
    maybeFinishSmoke();
  });
  ipcMain.handle("yakshed:request", async (event, request) => {
    assertTrustedRenderer(event);
    if (!request || typeof request !== "object" || Array.isArray(request)) throw new Error("invalid product request");
    const params = validateRpcRequest(request.method, request.params);
    if (!backend) throw new Error("YakShed backend is unavailable");
    try {
      const result = await backend.request(request.method, params);
      if (request.method === "adapter.login") registerLoginResult(result);
      updateRuntimeSettings(result);
      updateActiveCountFromResult(result);
      updateTray();
      return result;
    } catch (error) {
      throw new Error(safeErrorMessage(error));
    }
  });
  ipcMain.handle("yakshed:choose-workspace", async (event) => {
    assertTrustedRenderer(event);
    const owner = BrowserWindow.getFocusedWindow() || mainWindow;
    const result = await dialog.showOpenDialog(owner, { title: "Choose workspace", properties: ["openDirectory", "createDirectory"] });
    if (result.canceled || !result.filePaths[0]) return null;
    return validateWorkspace(result.filePaths[0]);
  });
}

function assertTrustedRenderer(event) {
  if (!mainWindow || event.sender !== mainWindow.webContents || !event.senderFrame || event.senderFrame !== mainWindow.webContents.mainFrame || !isLocalRendererUrl(event.senderFrame.url)) {
    throw new Error("untrusted renderer");
  }
}

function validateWorkspace(candidate) {
  if (typeof candidate !== "string" || !path.isAbsolute(candidate) || candidate.length > 4_096) throw new Error("invalid workspace path");
  const resolved = realpathSync(candidate);
  if (!statSync(resolved).isDirectory()) throw new Error("workspace must be a directory");
  if (resolved === path.parse(resolved).root) throw new Error("workspace must be a non-root directory");
  return resolved;
}

function registerLoginResult(result) {
  if (!result || typeof result !== "object") return;
  for (const key of ["login_url", "loginUrl"]) {
    const value = result[key];
    if (typeof value !== "string") continue;
    try {
      const url = new URL(value);
      if (url.protocol !== "https:" || !url.hostname || url.username || url.password) continue;
      delegatedLoginUrls.set(url.href, Date.now() + 10 * 60_000);
    } catch {
      // The backend response remains visible as a normal operation result.
    }
  }
}

function consumeDelegatedLogin(value) {
  try {
    const url = new URL(value);
    const expiry = delegatedLoginUrls.get(url.href);
    if (!expiry || expiry < Date.now() || url.protocol !== "https:" || url.username || url.password) return false;
    delegatedLoginUrls.delete(url.href);
    return true;
  } catch {
    return false;
  }
}

function updateRuntimeSettings(result) {
  if (!result || typeof result !== "object") return;
  updateActiveCountFromResult(result);
  const settings = result.settings && typeof result.settings === "object" ? result.settings : null;
  if (!settings) return;
  if (typeof settings.stay_awake === "boolean") {
    stayAwakePreference = settings.stay_awake;
    setStayAwake(stayAwakePreference && activeRunCount > 0);
  }
  if (typeof settings.menubar === "boolean") {
    menubarEnabled = settings.menubar;
    updateTray();
  }
}

function setStayAwake(enabled) {
  if (stayAwake === enabled) return;
  stayAwake = enabled;
  if (enabled && stayAwakeBlocker === undefined) stayAwakeBlocker = powerSaveBlocker.start("prevent-app-suspension");
  if (!enabled && stayAwakeBlocker !== undefined) {
    powerSaveBlocker.stop(stayAwakeBlocker);
    stayAwakeBlocker = undefined;
  }
}

function updateActiveCountFromResult(result) {
  if (!result || typeof result !== "object") return;
  const value = result.active_count ?? result.activeCount;
  if (Number.isSafeInteger(value) && value >= 0) {
    activeRunCount = value;
    setStayAwake(stayAwakePreference && activeRunCount > 0);
  }
}

function updateActiveCount(_event) {
  // The backend may add an active_count hint without making it part of the
  // durable event contract. Ignore malformed hints and retain local count.
  if (_event && Number.isSafeInteger(_event.active_count) && _event.active_count >= 0) activeRunCount = _event.active_count;
  setStayAwake(stayAwakePreference && activeRunCount > 0);
  updateTray();
}

function updateTray() {
  if (!menubarEnabled) {
    tray?.destroy();
    tray = undefined;
    return;
  }
  if (!tray) {
    tray = new Tray(nativeImage.createEmpty());
    tray.setTitle("YS");
    tray.setToolTip(APP_NAME);
    tray.on("click", () => mainWindow?.show());
  }
  tray.setContextMenu(Menu.buildFromTemplate([
    { label: `${APP_NAME} · ${activeRunCount} active`, enabled: false },
    { label: "Show YakShed", click: () => mainWindow?.show() },
    { type: "separator" },
    { label: "Quit YakShed", click: () => beginQuit() },
  ]));
}

function sendEvent(event) {
  if (mainWindow && !mainWindow.isDestroyed()) mainWindow.webContents.send("yakshed:event", event);
}

function startSmokeCheck() {
  smokeTimer = setTimeout(() => failSmoke("packaged renderer/service readiness timed out"), 30_000);
  if (startupError) {
    failSmoke(startupError);
    return;
  }
  backend.request("snapshot").then(() => backend.request("adapter.status", { adapter: "codex" })).then((status) => {
    if (!status || typeof status !== "object" || status.installed !== true || typeof status.version !== "string" || !status.version || typeof status.authenticated !== "boolean" || status.error != null) {
      throw new Error("packaged Codex runtime status unavailable");
    }
    smokeBackendReady = true;
    maybeFinishSmoke();
  }).catch((error) => failSmoke(safeErrorMessage(error, "packaged service status failed")));
}

function maybeFinishSmoke() {
  if (!smoke || !smokeBackendReady || !smokeRendererReady) return;
  if (smokeFinishTimer) return;
  smokeFinishTimer = setTimeout(() => {
    smokeFinishTimer = undefined;
    void beginQuit(0);
  }, 500);
}

function failSmoke(message) {
  if (quitStarted) return;
  sendEvent({ event: "backend_error", message: safeErrorMessage(message, "packaged smoke failed") });
  void beginQuit(1);
}

async function beginQuit(exitCode = 0) {
  if (quitStarted) return;
  quitStarted = true;
  quitting = true;
  if (smokeTimer) {
    clearTimeout(smokeTimer);
    smokeTimer = undefined;
  }
  if (smokeFinishTimer) {
    clearTimeout(smokeFinishTimer);
    smokeFinishTimer = undefined;
  }
  if (stayAwakeBlocker !== undefined) {
    powerSaveBlocker.stop(stayAwakeBlocker);
    stayAwakeBlocker = undefined;
  }
  tray?.destroy();
  tray = undefined;
  try {
    await backend?.stop();
  } finally {
    if (exitCode) app.exit(exitCode);
    else app.quit();
  }
}

app.on("before-quit", (event) => {
  if (quitting) return;
  event.preventDefault();
  void beginQuit();
});

app.on("window-all-closed", () => {
  if (!menubarEnabled) void beginQuit();
});

app.on("activate", () => createWindow());
