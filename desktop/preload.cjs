const { contextBridge, ipcRenderer } = require("electron");

const request = (method, params) => ipcRenderer.invoke("yakshed:request", { method, params });

window.addEventListener("DOMContentLoaded", () => {
  if (document.getElementById("app")?.children.length) ipcRenderer.send("yakshed:renderer-ready");
}, { once: true });

contextBridge.exposeInMainWorld("yakshed", Object.freeze({
  request,
  chooseWorkspace: () => ipcRenderer.invoke("yakshed:choose-workspace"),
  onEvent(callback) {
    if (typeof callback !== "function") throw new TypeError("event callback must be a function");
    const listener = (_event, payload) => callback(payload);
    ipcRenderer.on("yakshed:event", listener);
    return () => ipcRenderer.removeListener("yakshed:event", listener);
  },
}));
