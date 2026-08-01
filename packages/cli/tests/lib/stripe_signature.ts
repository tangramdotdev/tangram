import { createHmac } from "node:crypto";

const secret = process.argv[2];
const timestamp = process.argv[3];
const payload = process.argv[4];

if (secret === undefined || timestamp === undefined || payload === undefined) {
	throw new Error("expected the secret, timestamp, and payload");
}

const signature = createHmac("sha256", secret)
	.update(`${timestamp}.${payload}`)
	.digest("hex");

process.stdout.write(`t=${timestamp},v1=${signature}`);
