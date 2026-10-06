use ../lib/test.nu *

# A process and a sandbox can read their own nodes without authorization searches.

let root_token = random chars
let local = server spawn --preserve-keys --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	verification: { permissions: { initial: false, final: false } },
}
let path = artifact { tangram.ts: 'export default () => 42;' }
let process = tg --token $root_token build --detach --no-tokens $path | referent node
success (tg --token $root_token wait $process | complete)
let sandbox = tg --token $root_token process get $process | from json | get sandbox

# Sign authentication tokens with the same test key as the server.
let signer = artifact {
	'sign.mjs': '
		import { createPrivateKey, sign } from "node:crypto";
		const encode = (bytes) => {
			const alphabet = "0123456789abcdefghjkmnpqrstvwxyz";
			let output = "", bits = 0, value = 0;
			for (const byte of bytes) {
				value = (value << 8) | byte;
				bits += 8;
				while (bits >= 5) {
					bits -= 5;
					output += alphabet[(value >>> bits) & 31];
				}
			}
			if (bits) output += alphabet[(value << (5 - bits)) & 31];
			return output;
		};
		const now = Math.floor(Date.now() / 1000);
		const body = { expires_at: now + 60, issued_at: now, principal: { kind: process.argv[2], value: process.argv[3] } };
		const metadata = { algorithm: "ed25519", key: "default" };
		const input = `authentication.0.${encode(Buffer.from(JSON.stringify(body)))}.${encode(Buffer.from(JSON.stringify(metadata)))}`;
		const seed = Buffer.from("U9ZBC697GDA0dlUBF/VVM4eqoJUVfQqwRNr6L2z8Ajg=", "base64");
		const key = createPrivateKey({ key: Buffer.concat([Buffer.from("302e020100300506032b657004220420", "hex"), seed]), format: "der", type: "pkcs8" });
		console.log(`${input}.${encode(sign(null, Buffer.from(input), key))}`);
	'
} | path join sign.mjs

let process_token = node $signer process $process | str trim
let sandbox_token = node $signer sandbox $sandbox | str trim
success (tg --token $process_token process get $process | complete) "a process should read its own node without a search"
success (tg --token $sandbox_token sandbox get $sandbox | complete) "a sandbox should read its own node without a search"
success (tg --token $sandbox_token process get $process | complete) "a sandbox should retain access to its processes"

# Self authorization must not grant access to a different process or sandbox.
let other = tg --token $root_token build --cached=false --detach --no-tokens $path | referent node
success (tg --token $root_token wait $other | complete)
let other_sandbox = tg --token $root_token process get $other | from json | get sandbox
assert ($other != $process)
assert ($other_sandbox != $sandbox)
failure (tg --token $process_token process get $other | complete) "a process should not read another process"
failure (tg --token $sandbox_token sandbox get $other_sandbox | complete) "a sandbox should not read another sandbox"
failure (tg --token $sandbox_token process get $other | complete) "a sandbox should not read another sandbox's process"
