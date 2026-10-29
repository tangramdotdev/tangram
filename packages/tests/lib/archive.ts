const portPath = process.argv[2];
if (portPath === undefined) {
	throw new Error("expected the port path");
}

const requests: { body: string; path: string }[] = [];
const pending: ((status: number) => void)[] = [];
const waiters: { count: number; resolve: () => void }[] = [];
const server = Bun.serve({
	hostname: "127.0.0.1",
	idleTimeout: 0,
	port: 0,
	async fetch(request) {
		const path = new URL(request.url).pathname;
		if (request.method === "PUT") {
			const body = Buffer.from(await request.arrayBuffer()).toString("hex");
			requests.push({ body, path });
			for (const waiter of waiters.filter(
				(waiter) => waiter.count <= requests.length,
			)) {
				waiters.splice(waiters.indexOf(waiter), 1);
				waiter.resolve();
			}
			const status = await new Promise<number>((resolve) =>
				pending.push(resolve),
			);
			return new Response(null, { status });
		}
		if (request.method === "GET" && path === "/requests") {
			// Respond once at least the requested number of uploads have arrived.
			const count = Number(new URL(request.url).searchParams.get("count") ?? 0);
			if (requests.length < count) {
				await new Promise<void>((resolve) => waiters.push({ count, resolve }));
			}
			return Response.json(requests);
		}
		if (request.method === "POST" && path === "/respond") {
			const status = Number(await request.text());
			const respond = pending.shift();
			if (respond === undefined) {
				return new Response("no pending upload", { status: 409 });
			}
			respond(status);
			return new Response(null, { status: 204 });
		}
		return new Response(null, { status: 404 });
	},
});

await Bun.write(portPath, server.port.toString());
