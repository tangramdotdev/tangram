const portPath = process.argv[2];
if (portPath === undefined) {
	throw new Error("expected the port path");
}

const requests: { body: string; path: string }[] = [];
const pending: ((status: number) => void)[] = [];
const server = Bun.serve({
	hostname: "127.0.0.1",
	port: 0,
	async fetch(request) {
		const path = new URL(request.url).pathname;
		if (request.method === "PUT") {
			const body = Buffer.from(await request.arrayBuffer()).toString("hex");
			requests.push({ body, path });
			const status = await new Promise<number>((resolve) =>
				pending.push(resolve),
			);
			return new Response(null, { status });
		}
		if (request.method === "GET" && path === "/requests") {
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
