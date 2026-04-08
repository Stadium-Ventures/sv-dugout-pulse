// SV Dugout Pulse — StatBroadcast proxy
// Forwards requests from /<path> to https://stats.statbroadcast.com/<path>.
// CF→CF egress avoids the IP-reputation block that hits GitHub Actions.

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const target = new URL(url.pathname + url.search, "https://stats.statbroadcast.com");

    // Clone incoming headers, drop CF/host-specific ones the upstream shouldn't see.
    const headers = new Headers(request.headers);
    headers.delete("host");
    headers.delete("cf-connecting-ip");
    headers.delete("cf-ipcountry");
    headers.delete("cf-ray");
    headers.delete("cf-visitor");
    headers.delete("x-forwarded-for");
    headers.delete("x-forwarded-proto");
    headers.delete("x-real-ip");
    headers.set("host", "stats.statbroadcast.com");

    const upstream = await fetch(target.toString(), {
      method: request.method,
      headers,
      body: request.method === "GET" || request.method === "HEAD" ? undefined : request.body,
      redirect: "manual",
    });

    // Pass response through unchanged so x-sb-* headers, set-cookie, body all survive.
    const respHeaders = new Headers(upstream.headers);
    respHeaders.set("access-control-allow-origin", "*");
    return new Response(upstream.body, {
      status: upstream.status,
      statusText: upstream.statusText,
      headers: respHeaders,
    });
  },
};
