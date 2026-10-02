import { readFileSync, writeFileSync } from "node:fs";
import { networkInterfaces } from "node:os";

type ResponseConfig = {
	body?: string;
	file?: string;
	headers?: Record<string, string>;
	status?: number;
};

const [routesPath, portPath] = process.argv.slice(2);
if (routesPath === undefined || portPath === undefined) {
	throw new Error("expected the routes and port paths");
}
const routes: Record<string, ResponseConfig> = JSON.parse(
	readFileSync(routesPath, "utf8"),
);
const server = Bun.serve({
	hostname: "0.0.0.0",
	port: 0,
	fetch(request) {
		const path = new URL(request.url).pathname;
		const response = Object.hasOwn(routes, path) ? routes[path] : undefined;
		if (response === undefined) {
			return new Response("not found\n", { status: 404 });
		}
		const body =
			response.file === undefined
				? (response.body ?? "")
				: Bun.file(response.file);
		return new Response(body, {
			headers: response.headers,
			status: response.status ?? 200,
		});
	},
});
const port = server.port;
if (port === undefined) {
	throw new Error("expected the local HTTP server to listen on a TCP port");
}
const hostname = Object.values(networkInterfaces())
	.flatMap((addresses) => addresses ?? [])
	.find((address) => address.family === "IPv4" && !address.internal)?.address;
writeFileSync(portPath, JSON.stringify({ hostname, port }));
