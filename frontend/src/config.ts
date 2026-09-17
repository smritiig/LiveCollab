const configuredApiBaseUrl = import.meta.env.VITE_API_BASE_URL as string | undefined;

export const API_BASE_URL = (configuredApiBaseUrl || "http://localhost:8080").replace(/\/$/, "");
export const WEBSOCKET_BASE_URL = API_BASE_URL.replace(/^http/, "ws");
