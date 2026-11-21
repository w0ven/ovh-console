/** 优先使用浏览器真实 origin（避免反代终止 TLS 后后端拼接出 http://），无 origin 时回落后端返回。 */
export function buildPairUrl(code?: string, fallbackUrl?: string, origin?: string): string {
  const effectiveOrigin =
    origin !== undefined ? origin : typeof window !== "undefined" ? window.location?.origin || "" : "";
  if (code && effectiveOrigin) {
    return `${effectiveOrigin}/api/app/pair#${code}`;
  }
  return fallbackUrl || "";
}
