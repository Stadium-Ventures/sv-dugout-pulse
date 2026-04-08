// SV Dugout Pulse — StatBroadcast proxy (Deno Deploy)
// Forwards /<path> to https://stats.statbroadcast.com/<path>.

Deno.serve(async (req: Request) => {
  const url = new URL(req.url);
  const target = "https://stats.statbroadcast.com" + url.pathname + url.search;

  const headers = new Headers(req.headers);
  for (const h of [
    "host", "cf-connecting-ip", "cf-ipcountry", "cf-ray", "cf-visitor",
    "x-forwarded-for", "x-forwarded-proto", "x-real-ip",
  ]) headers.delete(h);

  const upstream = await fetch(target, {
    method: req.method,
    headers,
    body: req.method === "GET" || req.method === "HEAD" ? undefined : req.body,
    redirect: "manual",
  });

  const respHeaders = new Headers(upstream.headers);
  respHeaders.set("access-control-allow-origin", "*");
  return new Response(upstream.body, {
    status: upstream.status,
    statusText: upstream.statusText,
    headers: respHeaders,
  });
});
