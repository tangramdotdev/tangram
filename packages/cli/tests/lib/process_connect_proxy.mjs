import { appendFileSync, writeFileSync } from "node:fs";
import http from "node:http";
import http2 from "node:http2";

const [socketPath, port, log, ready] = process.argv.slice(2);
const server = http2.createServer();
server.on("stream", (stream, headers) => {
	const path = headers[":path"];
	if (path.startsWith("/processes/")) appendFileSync(log, `${path}\n`);
	const request = http.request(
		{
			headers: Object.fromEntries(
				Object.entries(headers).filter(([name]) => !name.startsWith(":")),
			),
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
			stream.respond({ ...headers, ":status": response.statusCode });
			response.on("error", () => stream.close());
			response.pipe(stream);
		},
	);
	request.on("error", () => stream.close());
	stream.on("error", () => request.destroy());
	stream.on("close", () => request.destroy());
	request.flushHeaders();
	stream.pipe(request);
});
server.listen(Number(port), "127.0.0.1", () => writeFileSync(ready, "ready"));
