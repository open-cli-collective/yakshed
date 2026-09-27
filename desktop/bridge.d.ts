export type YakShedEvent = {
  event: "changed" | "backend_error" | "preview";
  revision?: number;
  active_count?: number;
  message?: string;
  enabled?: boolean;
};

export type YakShedBridge = {
  request<T = unknown>(method: string, params?: Record<string, unknown>): Promise<T>;
  onEvent(callback: (event: YakShedEvent) => void): () => void;
  chooseWorkspace(): Promise<string | null>;
};

declare global {
  interface Window {
    yakshed: YakShedBridge;
  }
}
