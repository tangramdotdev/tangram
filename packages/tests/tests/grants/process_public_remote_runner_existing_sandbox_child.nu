use ../../util/test.nu *

# A public child that a process on a remote's runner spawns in its own sandbox is public at the remote, which is the child's home, and its parent can wait on it there.

let root_token = random chars
let remote = server spawn --name remote --cloud --config {
	advanced: { multi_server: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [indexer runner],
	runner: { id: $created.data.id, remote: 'default', token: $created.token.token },
}
let local = server spawn --name local --config {
	remotes: { default: { token: $root_token, url: $remote.url } },
}

# Run a command as an anonymous client.
let config = mktemp
{} | to json | save -f $config
let anonymous = {|command| with-env { TANGRAM_CONFIG: $config } { do $command | complete } }

# The parent spawns a public child in its own sandbox, waits on it, and returns its ID.
let path = artifact {
	tangram.ts: '
		export default async function (sandbox: string) {
			const command = await tg.Command.javascriptArg(child, []);
			const { arg } = await tg.Process.spawnArg(
				{ sandbox, stderr: "log", stdin: "null", stdout: "log", tty: false },
				{ command },
			);
			delete arg.tty;
			arg.public = true;
			const output = await tg.Progress.lastOutput(await tg.client.spawnProcess(arg));
			const location = output.location === undefined || output.location === null ? null : tg.Location.Arg.fromLocation(output.location);
			const outcome = await tg.client.waitProcess(output.process, { lease: output.lease, location, tokens: output.tokens });
			if (outcome.exit !== 0) {
				throw new Error("the child failed");
			}
			return output.process;
		}
		export function child() {
			return 42;
		}
	'
}
let artifact = tg --url $local.url checkin --no-tokens $path | referent node
tg --url $local.url push $artifact | ignore
let sandbox = tg --url $remote.url --token $root_token sandbox create --wait --no-tokens | referent node
let process = tg --url $remote.url --token $root_token spawn --no-tokens $'--sandbox=($sandbox)' --arg-string $sandbox $artifact | referent node
let wait = tg --url $remote.url --token $root_token wait $process | complete
success $wait 'the parent should wait on its child'
let outcome = $wait.stdout | from json
assert equal $outcome.exit 0 'the parent should wait on its public child at the remote'
let child = $outcome.output
assert equal (tg --url $remote.url --token $root_token process get $child | from json | get sandbox) $sandbox 'the child should run in its parent sandbox'

# An anonymous client can read the public child at the remote, and not its private parent.
tg --url $remote.url --token $root_token index
failure (do $anonymous { tg --url $remote.url process get $process }) 'an anonymous client must not read the private parent'
let get = do $anonymous { tg --url $remote.url process get $child }
success $get 'an anonymous client should read the public child at the remote'
assert equal ($get.stdout | from json | get status) finished
tg --url $remote.url --token $root_token sandbox destroy $sandbox
