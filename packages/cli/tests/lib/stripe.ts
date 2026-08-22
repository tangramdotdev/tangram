import { appendFileSync } from "node:fs";

const portPath = process.argv[2];
const requestsPath = process.argv[3];
const customerDelay = Number(process.argv[4] ?? 0);

if (portPath === undefined || requestsPath === undefined) {
	throw new Error("expected the port and requests paths");
}

const server = Bun.serve({
	port: 0,
	async fetch(request) {
		const body = await request.text();
		const url = new URL(request.url);
		const record = {
			authorization: request.headers.get("authorization"),
			body,
			idempotencyKey: request.headers.get("idempotency-key"),
			method: request.method,
			path: url.pathname,
		};
		appendFileSync(requestsPath, `${JSON.stringify(record)}\n`);
		if (request.method === "POST" && url.pathname === "/v1/customers") {
			await Bun.sleep(customerDelay);
			return Response.json({ id: "cus_mock" });
		}
		if (
			request.method === "POST" &&
			url.pathname === "/v1/billing_portal/sessions"
		) {
			return Response.json({ url: "https://example.invalid/stripe-portal" });
		}
		if (request.method === "GET" && url.pathname === "/v1/customers/cus_mock") {
			return Response.json({
				id: "cus_mock",
				invoice_settings: { default_payment_method: "pm_mock" },
			});
		}
		return Response.json(
			{ error: { message: `unexpected path: ${url.pathname}` } },
			{ status: 404 },
		);
	},
});

await Bun.write(portPath, server.port.toString());
await new Promise(() => {});
