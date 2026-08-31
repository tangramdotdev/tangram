use ../../test.nu *

# A runner sandbox cannot use the runner's configured remote for an unrelated remote API.

let root_token = random chars
let remote = server spawn --cloud --name remote --config {
	authentication: { root: { token: $root_token } },
	roles: [http indexer scheduler],
}

let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [indexer runner],
	runner: { id: $created.runner.id, remote: "default", token: $created.token.token },
}

let local = server spawn --name local --config {
	remotes: { default: { token: $root_token, url: $remote.url } },
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			const process = await tg.spawn({
				args: ["group", "create", "--remote", "forbidden"],
			executable: "tg",
		})
			.stderr("pipe")
			.stdin("null")
			.stdout("null")
			.sandbox();
			const [stderr, wait] = await Promise.all([process.stderr.text(), process.wait()]);
			return wait.exit !== 0 && stderr.includes("network access is disabled for the origin sandbox");
		}
	'
}

let output = tg --url $local.url build --remote $path | from json
assert equal $output true
