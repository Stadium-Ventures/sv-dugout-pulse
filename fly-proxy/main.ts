// SV Dugout Pulse — StatBroadcast proxy (Fly.io)
// Forwards /<path> to https://stats.statbroadcast.com/<path>.
// Runs on Fly.io's egress IPs, which aren't on StatBroadcast's WAF blocklist
// (unlike Cloudflare Worker egress, which started 403ing on 2026-04-17).

const PORT = Number(Deno.env.get("PORT")) || 8080;

Deno.serve({ port: PORT }, async (req: Request) => {
  const url = new URL(req.url);

  // Healthcheck so Fly can verify the machine is live before routing traffic.
  if (url.pathname === "/__health") {
    return new Response("ok", { status: 200 });
  }

  const target = "https://stats.statbroadcast.com" + url.pathname + url.search;

  const headers = new Headers(req.headers);
  for (
    const h of [
      "host",
      "cf-connecting-ip",
      "cf-ipcountry",
      "cf-ray",
      "cf-visitor",
      "x-forwarded-for",
      "x-forwarded-proto",
      "x-real-ip",
      "fly-client-ip",
      "fly-forwarded-proto",
      "fly-region",
      "fly-request-id",
    ]
  ) headers.delete(h);

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
