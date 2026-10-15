import { appendFileSync, writeFileSync } from "node:fs";
import http2 from "node:http2";
import net from "node:net";

// Forward HTTP/2 streams to a server's socket, resetting the first stdio stream of the given kind once it has carried more than the threshold.
const [socketPath, port, kind, threshold, log, ready] = process.argv.slice(2);
const upstream = http2.connect("http://localhost", {
	createConnection: () => net.connect(socketPath),
});
const pattern = new RegExp(`^/processes/[^/?]+/stdio/${kind}[?]`);
let reset = false;
const server = http2.createServer();
server.on("stream", (stream, headers) => {
	const path = headers[":path"];
	const request = upstream.request(
		Object.fromEntries(
			Object.entries(headers).filter(
				([name]) => !["connection", "keep-alive"].includes(name),
			),
		),
	);
	const target = !reset && pattern.test(path);
	let carried = 0;
	const carry = (chunk) => {
		carried += chunk.length;
		if (target && !reset && carried > Number(threshold)) {
			reset = true;
			appendFileSync(log, `reset ${path}\n`);
			stream.close(http2.constants.NGHTTP2_INTERNAL_ERROR);
			request.close(http2.constants.NGHTTP2_CANCEL);
			return false;
		}
		return true;
	};
	let trailers = {};
	request.on("response", (headers) => {
		if (!stream.destroyed) stream.respond(headers, { waitForTrailers: true });
	});
	request.on("trailers", (headers) => {
		trailers = headers;
	});
	stream.on("wantTrailers", () => stream.sendTrailers(trailers));
	request.on("data", (chunk) => {
		if (stream.destroyed) return;
		if (kind === "read" && !carry(chunk)) return;
		stream.write(chunk);
	});
	request.on("end", () => {
		if (!stream.destroyed) stream.end();
	});
	request.on("error", () => {
		if (!stream.destroyed) stream.close(http2.constants.NGHTTP2_INTERNAL_ERROR);
	});
	stream.on("data", (chunk) => {
		if (request.destroyed) return;
		if (kind === "write" && !carry(chunk)) return;
		request.write(chunk);
	});
	stream.on("end", () => request.end());
	stream.on("error", () => {
		if (!request.closed) request.close(http2.constants.NGHTTP2_CANCEL);
	});
	stream.on("close", () => {
		if (!request.closed) request.close(http2.constants.NGHTTP2_CANCEL);
	});
});
server.listen(Number(port), "127.0.0.1", () => writeFileSync(ready, "ready"));
