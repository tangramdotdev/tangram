use ../lib/test.nu *

const js_path = path self '../../../js'

# An unsandboxed inline spawn command authorizes its private executable from its referent tokens without traversing the authorization graph.

let server = server spawn --preserve-keys --config {
	authentication: { users: { providers: { insecure: true } } }
}

let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json

# Create a private executable and retain its exact subtree tokens.
let producer = artifact {
	tangram.ts: '
		export default async function () {
			const executable = await tg.file("#!/bin/sh\nif [ \"$FAST\" = 1 ]; then exit 0; fi\nIFS= read -r input\n[ \"$input\" = \"input data\" ]", { executable: true });
			await tg.Value.store(executable);
			return {
				executable: { node: executable.id, options: { tokens: executable.state.tokens } },
			};
		}
	'
}
let inputs = tg --token $alice.token build $producer | from json
let executable_referent = $inputs.executable
failure (tg --token $bob.token get $executable_referent.node | complete) 'Bob must not have a direct grant for the executable'

# Disable authorization graph searches.
let config = $server.config | merge deep {
	authorization: {
		final: false
		index: { delay: null }
		initial: false
	}
}
$config | to json | save --force $server.config_path
let server = $server | upsert config $config
let server = server restart $server

# Unsandboxed spawning happens in the client, not on the server.
cd $js_path
for mode in [none executable] {
	let output = with-env { TANGRAM_TOKEN: $bob.token, TOKEN_MODE: $mode, EXECUTABLE_REFERENT: ($executable_referent | to json --raw) } {
		node --input-type=module -e '
			import * as tg from "@tangramdotdev/client";
			const decoder = new TextDecoder();
			const encoder = new TextEncoder();
			tg.setEncoding({
				json: { decode: JSON.parse, encode: JSON.stringify },
				utf8: { decode: (value) => decoder.decode(value), encode: (value) => encoder.encode(value) },
			});
			tg.setProcess({
				args: process.argv.slice(2),
				cwd: process.cwd(),
				env: Object.fromEntries(Object.entries(process.env).filter(([, value]) => value !== undefined)),
				executable: process.execPath,
			});
			const referent = JSON.parse(process.env.EXECUTABLE_REFERENT);
			const executable = process.env.TOKEN_MODE === "executable"
				? tg.File.withReferent(referent)
				: tg.File.withId(referent.node);
			const child = await tg.spawn({ env: { FAST: "1" }, executable }).stdio("null");
			const wait = await child.wait();
			if (wait.exit !== 0) throw new Error("the process failed");
			process.stdout.write("spawned");
			process.exit(0);
		' | complete
	}
	if $mode == none {
		failure $output 'the unsandboxed inline command without the executable token must not be authorized'
	} else {
		success $output 'the unsandboxed inline command with the executable token must be authorized'
		assert equal $output.stdout 'spawned'
	}
}
