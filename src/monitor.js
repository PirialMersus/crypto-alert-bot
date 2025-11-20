// src/monitor.js
export let _lastHeartbeat = null;
export function setLastHeartbeat(iso) {
  _lastHeartbeat = iso || new Date().toISOString();
}
export default { setLastHeartbeat };
