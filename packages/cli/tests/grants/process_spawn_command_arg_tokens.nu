use ../lib/test.nu *

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
			const executable = await tg.file("#!/bin/sh\nif [ \"$FAST\" = 1 ]; then exit 0; fi\nIFS= read -r input\n[ \"$input\" = \"input data\" ]", { executable: true });
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

# Spawn inline commands directly so this authorization test does not depend on the Node.js client.
let socket = $server.url | str replace 'http+unix://' '' | url decode
let headers = { Authorization: $'Bearer ($bob.token)', 'Content-Type': 'application/json' }

for mode in [none stdin executable both] {
	let executable_options = if $mode in [executable both] { $executable_referent.options } else { {} }
	let stdin_options = if $mode in [stdin both] { $stdin_referent.options } else { {} }
	let arg = {
		command: {
			node: {
				executable: { node: { artifact: $executable_referent.node }, options: $executable_options }
				stdin: { node: $stdin_referent.node, options: $stdin_options }
			}
		}
		sandbox: { ttl: 0 }
		stdin: 'pipe'
		stdout: 'null'
		stderr: 'null'
	} | to json --raw
	let response = http post --full --allow-errors --raw --max-time 30sec --unix-socket $socket --headers $headers 'http://localhost/processes/spawn' $arg

	assert equal $response.status 200 'the spawn request must be accepted'
	assert (not ($response.body | str contains 'event: error')) 'the spawn must return a process'
	let output = $response.body | lines | where { $in starts-with 'data: ' } | last | str substring 6.. | from json
	let token = $output.tokens.local.0
	let reference = $'($output.process)?tokens[local][0]=($token | url encode --all)'
	let outcome = tg --token $bob.token wait $reference | from json
	if $mode == both {
		assert equal $outcome.exit 0 'the authorized process must read its private stdin and finish successfully'
	} else {
		assert ($outcome.exit != 0) $'the inline command with token mode ($mode) must not be authorized'
	}
}
