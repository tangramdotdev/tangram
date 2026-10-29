use ../lib/test.nu *

# A process and a sandbox can read their own nodes without authorization searches.

let root_token = random chars
let secret_path = $env.PWD | path join secret
'U9ZBC697GDA0dlUBF/VVM4eqoJUVfQqwRNr6L2z8Ajg=' | decode base64 | save -f $secret_path
let secret = { algorithm: hmac_sha256, id: 0, path: $secret_path }
let local = server spawn --config {
	authentication: { root: { token: $root_token }, tokens: { secret: $secret }, users: { providers: { insecure: true } } },
	verification: { permissions: { initial: false, final: false } },
}
let path = artifact { tangram.ts: 'export default () => 42;' }
let process = tg --token $root_token build --detach --no-tokens $path | referent node
success (tg --token $root_token wait $process | complete)
let sandbox = tg --token $root_token process get $process | from json | get sandbox

# Sign authentication tokens with the same secret as the server.
let signer = artifact {
	'sign.mjs': '
		import { createHmac } from "node:crypto";
		const symbols = "0123456789abcdefghjkmnpqrstvwxyz";
		const encode = (bytes) => {
			let output = "", bits = 0, buffer = 0;
			for (const byte of bytes) {
				buffer = ((buffer << 8) | byte) & 0xfff;
				bits += 8;
				while (bits >= 5) {
					bits -= 5;
					output += symbols[(buffer >> bits) & 31];
				}
			}
			if (bits) output += symbols[(buffer << (5 - bits)) & 31];
			return output;
		};
		const decode = (value) => {
			const bytes = [];
			let bits = 0, buffer = 0;
			for (const character of value) {
				buffer = ((buffer << 5) | symbols.indexOf(character)) & 0xfff;
				bits += 5;
				if (bits >= 8) {
					bits -= 8;
					bytes.push((buffer >> bits) & 0xff);
				}
			}
			return bytes;
		};
		const uvarint = (value) => {
			const bytes = [];
			while (value > 0x7f) {
				bytes.push((value & 0x7f) | 0x80);
				value = Math.floor(value / 128);
			}
			bytes.push(value);
			return bytes;
		};
		const id = process.argv[2];
		const kind = { pcs: 8, sbx: 7 }[id.slice(0, 3)];
		const principal = [0, 0, kind, Number(id[5]), ...decode(id.slice(6))];
		const input = Buffer.from([0, 0, ...uvarint(Math.floor(Date.now() / 1000) + 60), ...principal]);
		const secret = Buffer.from("U9ZBC697GDA0dlUBF/VVM4eqoJUVfQqwRNr6L2z8Ajg=", "base64");
		const mac = createHmac("sha256", secret).update(input).digest().subarray(0, 16);
		console.log(encode([...input, ...uvarint(0), ...mac]));
	'
} | path join sign.mjs

let process_token = node $signer $process | str trim
let sandbox_token = node $signer $sandbox | str trim
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
