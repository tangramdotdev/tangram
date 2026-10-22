import { writeFileSync } from "node:fs";
import net from "node:net";

// Forward each TCP connection to the next of the given server sockets in turn, like a layer 4 load balancer.
const [port, ready, ...sockets] = process.argv.slice(2);
let next = 0;
const server = net.createServer((downstream) => {
	const socket = sockets[next];
	next = (next + 1) % sockets.length;
	const upstream = net.connect(socket);
	downstream.pipe(upstream);
	upstream.pipe(downstream);
	downstream.on("error", () => upstream.destroy());
	upstream.on("error", () => downstream.destroy());
	downstream.on("close", () => upstream.destroy());
	upstream.on("close", () => downstream.destroy());
});
server.listen(Number(port), "127.0.0.1", () => writeFileSync(ready, "ready"));
