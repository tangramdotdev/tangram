use ../lib/test.nu *

# The Node process reader uses file contents for empty output, error, and outcome attributes.

const javascript_path = path self "../../../javascript"
let local = server spawn
let input = artifact {
	output: (file --xattrs { "user.tangram.output": '' } '"value"')
	error: (file --xattrs { "user.tangram.error": '' } '{"message":"failed"}')
	outcome: (file --xattrs { "user.tangram.outcome": '', "user.tangram.output": 'invalid' } '{"exit":0,"output":"value"}')
	missing: (file --xattrs { "user.tangram.outcome": '' } '{"exit":0}')
	"null": (file --xattrs { "user.tangram.outcome": '' } '{"exit":0,"output":null}')
	exit: (file --xattrs { "user.tangram.outcome": '' } '{"exit":7}')
}
cd $javascript_path
let output = node --input-type=module -e '
	import assert from "node:assert/strict";
	import * as tg from "@tangramdotdev/client";
	tg.setEncoding({
		utf8: { decode: (bytes) => new TextDecoder().decode(bytes), encode: (text) => new TextEncoder().encode(text) },
		json: { decode: JSON.parse, encode: JSON.stringify },
	});
	tg.setProcess({
		args: [], cwd: process.cwd(), env: process.env, executable: process.execPath,
	});
	for (const [name, exit] of [["output", 0], ["error", 0], ["outcome", 0], ["missing", 0], ["null", 0], ["exit", 0], ["outcome", 7]]) {
		const child = await tg.spawn({
			executable: "sh",
			args: ["-c", `cp -a "$1" "$TANGRAM_OUTPUT"; exit "$2"`, "_", `${process.argv[1]}/${name}`, String(exit)],
		}).sandbox(false);
		const outcome = await child.wait();
		if (name === "error") {
			assert(outcome.error !== null);
			assert.equal(outcome.output, undefined);
		} else {
			assert.equal(outcome.error, null);
			assert.equal(outcome.exit, exit);
			assert.equal(outcome.output, name === "null" ? null : ["output", "outcome"].includes(name) ? "value" : undefined);
		}
	}
	console.log("ok");
' $input | complete
success $output
snapshot ($output.stdout | str trim) 'ok'
