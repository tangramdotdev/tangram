use ../../test.nu *

const js_path = path self '../../../js'

# An inline spawn command authorizes its private executable and stdin directly from their referent tokens without traversing the authorization graph.

let server = server spawn --preserve-keys --config {
	authentication: { users: { providers: { insecure: true } } }
}

let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json

# Create private command inputs and retain their exact subtree tokens.
let producer = artifact {
	tangram.ts: '
		export default async function () {
			const executable = await tg.file("#!/bin/sh\nif [ \"$FAST\" = 1 ]; then exit 0; fi\nsleep 60", { executable: true });
			const stdin = await tg.blob("input data");
			await tg.Value.store([executable, stdin]);
			return {
				executable: { node: executable.id, options: { tokens: executable.state.tokens } },
				stdin: { node: stdin.id, options: { tokens: stdin.state.tokens } },
			};
		}
	'
}
let inputs = tg --token $alice.token build $producer | from json
let executable_referent = $inputs.executable
let stdin_referent = $inputs.stdin
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

# Spawn the same inline command with and without the input referent tokens as Bob.
let executable_referent_json = $executable_referent | to json
let stdin_referent_json = $stdin_referent | to json
let source = [
	'import * as tg from "@tangramdotdev/client";'
	''
	'const decoder = new TextDecoder();'
	'const encoder = new TextEncoder();'
	'tg.setEncoding({'
	'json: { decode: JSON.parse, encode: JSON.stringify },'
	'utf8: { decode: (value) => decoder.decode(value), encode: (value) => encoder.encode(value) },'
	'});'
	''
	'const env = Object.fromEntries('
	'Object.entries(process.env).filter(([, value]) => value !== undefined),'
	');'
	'tg.setProcess({'
	'args: process.argv.slice(2),'
	'cwd: process.cwd(),'
	'env,'
	'executable: process.execPath,'
	'});'
	''
	$'const executableReferent = ($executable_referent_json);'
	$'const stdinReferent = ($stdin_referent_json);'
	''
	'const mode = process.env.TOKEN_MODE;'
	'const executable = mode === "executable" || mode === "both"'
	'? tg.File.withReferent(executableReferent)'
	': tg.File.withId(executableReferent.node);'
	'const stdin = mode === "stdin" || mode === "both"'
	'? tg.Blob.withReferent(stdinReferent)'
	': tg.Blob.withId(stdinReferent.node);'
	'if (process.env.EXECUTION_MODE === "unsandboxed") {'
	'const child = await tg.spawn({ env: { FAST: "1" }, executable }).stdio("null");'
	'const wait = await child.wait();'
	'if (wait.exit !== 0) throw new Error("the process failed");'
	'} else {'
	'await tg.spawn({ executable, stdin }).stdout("null").sandbox();'
	'}'
	'process.stdout.write("spawned");'
	'process.exit(0);'
] | str join "\n"

cd $js_path

let output = with-env { TANGRAM_TOKEN: $bob.token, TOKEN_MODE: none } {
	node --input-type=module -e $source | complete
}
failure $output 'the inline command without referent tokens must not be authorized'

let output = with-env { TANGRAM_TOKEN: $bob.token, TOKEN_MODE: stdin } {
	node --input-type=module -e $source | complete
}
failure $output 'the inline command with only the stdin token must not be authorized'

let output = with-env { TANGRAM_TOKEN: $bob.token, TOKEN_MODE: executable } {
	node --input-type=module -e $source | complete
}
failure $output 'the inline command with only the executable token must not be authorized'

let output = with-env { TANGRAM_TOKEN: $bob.token, TOKEN_MODE: both } {
	node --input-type=module -e $source | complete
}
success $output 'the inline command with executable and stdin tokens must be authorized'
snapshot $output.stdout 'spawned'

let output = with-env { EXECUTION_MODE: unsandboxed, TANGRAM_TOKEN: $bob.token, TOKEN_MODE: none } {
	node --input-type=module -e $source | complete
}
failure $output 'the unsandboxed inline command without the executable token must not be authorized'

let output = with-env { EXECUTION_MODE: unsandboxed, TANGRAM_TOKEN: $bob.token, TOKEN_MODE: executable } {
	node --input-type=module -e $source | complete
}
success $output 'the unsandboxed inline command with the executable token must be authorized'
snapshot $output.stdout 'spawned'
