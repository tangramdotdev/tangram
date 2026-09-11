import assert from "node:assert/strict";
import http from "node:http";

const [socketPath, id, lease, mode] = process.argv.slice(2);
const timer = setTimeout(() => {
	throw new Error("the connect test timed out");
}, 10000);
try {
	await new Promise((resolve, reject) => {
		let connected = false;
		let detached = false;
		const request = http.request({
			socketPath,
			path: "/processes/connect",
			method: "POST",
			headers: {
				accept: "text/event-stream",
				"content-type": "text/event-stream",
			},
		});
		const send = (id, arg) =>
			request.write(`event: request\ndata: ${JSON.stringify({ arg, id })}\n\n`);
		request.on("error", (error) => {
			if (!connected || mode === "detach") reject(error);
		});
		request.on("response", (response) => {
			assert.equal(response.statusCode, 200);
			let buffer = "";
			response.setEncoding("utf8");
			response.on("data", (data) => {
				buffer += data;
				while (buffer.includes("\n\n")) {
					let index = buffer.indexOf("\n\n");
					let frame = buffer.slice(0, index);
					buffer = buffer.slice(index + 2);
					let event = frame
						.split("\n")
						.find((line) => line.startsWith("event:"))
						?.slice(6)
						.trim();
					let data = JSON.parse(
						frame
							.split("\n")
							.find((line) => line.startsWith("data:"))
							?.slice(5),
					);
					if (event === "error") {
						reject(new Error(JSON.stringify(data)));
						request.destroy();
						return;
					}
					if (event !== "response") continue;
					if (data.error) {
						reject(new Error(JSON.stringify(data.error)));
						request.destroy();
						return;
					}
					if (data.id === 0) {
						assert.equal(data.output.kind, "connect");
						connected = true;
						if (mode === "detach") send(1, { kind: "detach" });
						else {
							request.destroy();
							resolve();
						}
					} else if (data.id === 1) {
						assert.equal(data.output.kind, "detach");
						detached = true;
						request.end();
					}
				}
			});
			response.on("error", (error) => {
				if (!connected || mode === "detach") reject(error);
			});
			response.on("end", () => {
				if (mode === "detach") {
					if (!detached)
						reject(new Error("the connection ended before detach"));
					else resolve();
				}
			});
		});
		send(0, {
			kind: "connect",
			value: {
				reads: {},
				target: {
					kind: "existing",
					value: {
						id,
						...(lease === "none" ? {} : { lease }),
						location: "remote",
					},
				},
			},
		});
	});
} finally {
	clearTimeout(timer);
}
