import { appendFileSync, writeFileSync } from "node:fs";
import http from "node:http";
import http2 from "node:http2";

const [socketPath, port, log, ready, mode] = process.argv.slice(2);
const resets = new Set(
	mode?.startsWith("reset-first:")
		? mode.slice("reset-first:".length).split(",")
		: [],
);
const server = http2.createServer();
server.on("stream", (stream, headers) => {
	const path = headers[":path"];
	if (path.startsWith("/processes/")) appendFileSync(log, `${path}\n`);
	// Reset the first request to each of the given endpoints before forwarding it, like a network failure. A process endpoint is named by its last segment, a get of a process or a sandbox by its kind, and a sandbox endpoint by its last segment after "sandbox-".
	const segments = path.split("?")[0].split("/").filter(Boolean);
	const endpoint =
		segments[0] === "processes"
			? segments.length === 2
				? "process"
				: segments.at(-1)
			: segments[0] === "sandboxes" && segments.length >= 2
				? segments.length === 2
					? "sandbox"
					: `sandbox-${segments.at(-1)}`
				: null;
	if (resets.delete(endpoint)) {
		appendFileSync(log, `reset ${endpoint}\n`);
		stream.close();
		return;
	}
	// Give each bidirectional stream its own HTTP/1 connection.
	const request = http.request(
		{
			agent: false,
			headers: {
				...Object.fromEntries(
					Object.entries(headers).filter(([name]) => !name.startsWith(":")),
				),
				te: "trailers",
			},
			method: headers[":method"],
			path,
			socketPath,
		},
		(response) => {
			if (stream.destroyed) {
				response.destroy();
				return;
			}
			const headers = Object.fromEntries(
				Object.entries(response.headers).filter(
					([name]) =>
						!["connection", "keep-alive", "transfer-encoding"].includes(name),
				),
			);
			// Preserve native protocol error trailers.
			stream.respond(
				{ ...headers, ":status": response.statusCode },
				{ waitForTrailers: true },
			);
			stream.on("wantTrailers", () => stream.sendTrailers(response.trailers));
			response.on("error", (error) => {
				console.error(`the upstream response for ${path} failed: ${error}`);
				stream.close();
			});
			response.pipe(stream);
		},
	);
	request.on("error", (error) => {
		console.error(`the upstream request for ${path} failed: ${error}`);
		stream.close();
	});
	stream.on("error", () => request.destroy());
	stream.on("close", () => request.destroy());
	request.flushHeaders();
	stream.pipe(request);
});
server.listen(Number(port), "127.0.0.1", () => writeFileSync(ready, "ready"));
