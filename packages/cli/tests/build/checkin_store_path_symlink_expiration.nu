use ../lib/test.nu *

# Physical directory checkout tokens remain valid after the input authorization token expires, including graph symlink targets.

for case in [
	{ time: '2026-01-01T00:01:01Z', reuse: false },
	{ time: '2026-01-01T00:01:21Z', reuse: true },
] {
	let root_token = random chars
	let server = server spawn --now '2026-01-01T00:00:00Z' --config {
		advanced: { checkpoints: true }
		authentication: { root: { token: $root_token } }
		authorization: { final: false, initial: false }
		object: { permission_time_to_live: 60 }
		vfs: false
	}
	let path = artifact {
		tangram.ts: '
			export default async function () {
				const graph = await tg.graph({ nodes: [
					{ kind: "symlink", artifact: 1 },
					{ kind: "symlink", artifact: 2 },
					{ kind: "directory", entries: {} },
				] });
				const directory = await tg.directory({ graph, index: 2, kind: "directory" });
				return tg.command({
					args: ["-ec", `tg checkin "\${INPUT%/*}/${directory.id}"`],
					env: { INPUT: tg.symlink({ graph, index: 0, kind: "symlink" }) },
					executable: "/bin/sh",
					host: tg.host.current,
				});
			}
		'
	}
	let command = tg --token $root_token build $path | str trim | split row '?' | first
	let socket = $server.url | str replace 'http+unix://' '' | url decode
	let output = http get --headers { Authorization: $'Bearer ($root_token)', Accept: 'application/json' } --unix-socket $socket $'http://localhost/objects/($command)'
	let token = $output.tokens.local.0
	let reference = $'($command)?tokens[local][0]=($token | url encode --all)'
	let sandbox = tg --token $root_token sandbox create --no-network | str trim

	# Materialize the target before reusing the checkout with the older input authorization token.
	if $case.reuse {
		advance_time $server 20sec
		let output = http get --headers { Authorization: $'Bearer ($root_token)', Accept: 'application/json' } --unix-socket $socket $'http://localhost/objects/($command)'
		let token = $output.tokens.local.0
		let reference = $'($command)?tokens[local][0]=($token | url encode --all)'
		let output = tg --token $root_token run $'--sandbox=($sandbox)' $reference | complete
		success $output "the newer authorization token should seed the shared sandbox"
	}

	set_time $server '2026-01-01T00:00:40Z'
	let watch = tg --token $root_token checkpoint watch runner.process.start | from json | get watch
	let process = tg --token $root_token run $'--sandbox=($sandbox)' --detach $reference | str trim
	timeout 30s tg --token $root_token checkpoint wait runner.process.start $watch 0 | ignore
	set_time $server $case.time
	tg --token $root_token checkpoint continue runner.process.start $watch 0
	tg --token $root_token checkpoint unwatch runner.process.start $watch
	let output = timeout 30s tg --token $root_token wait $process | from json
	assert equal $output.exit 0
	tg --token $root_token sandbox destroy $sandbox
	server stop $server
}
