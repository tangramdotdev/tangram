use ../../util/test.nu *

# A process connection forwards requests to the selected location.

const proxy_path = path self ../../util/process_attach_proxy.js
let remote = server spawn --name remote
let port = port
let log = mktemp
let ready = mktemp -d | path join ready
^mkfifo $ready
let socket = $remote.directory | path join socket
let proxy = job spawn { node $proxy_path $socket $port $log $ready }
success (timeout 30s cat $ready | complete) "the proxy should listen"
let local = server spawn --name local --config { remotes: { default: { url: $'http://127.0.0.1:($port)' } } }
let path = artifact {
	tangram.ts: '
		export default async function () {
			let process =
				await tg.spawn`read line; echo "out:$line"; echo "err:$line" >&2`
					.stdio("pipe")
					.sandbox()
					.location({ components: [{ name: "default" }] })
					.connection("run");
			let [, stdout, stderr, outcome] = await Promise.all([
				process.stdin.writeAll(tg.encoding.utf8.encode("hello\n")),
				process.stdout.text(),
				process.stderr.text(),
				process.wait(),
			]);
			tg.assert(stdout === "out:hello\n");
			tg.assert(stderr === "err:hello\n");
			tg.assert(outcome.exit === 0);
			let spawned = await tg.spawn`echo attached`
				.stdin("null")
				.stdout("log")
				.stderr("null")
				.sandbox()
				.location({ components: [{ name: "default" }] })
				.connection("spawn");
			await spawned.detach();
			tg.assert(typeof spawned.id === "string");
			let attached = await tg.Process.attach(spawned.id, {
				lease: spawned.lease,
				location: spawned.location,
				tokens: spawned.tokens,
			});
			tg.assert((await attached.stdout.text()) === "attached\n");
			tg.assert((await attached.wait()).exit === 0);
			return [process.id, spawned.id];
		}
	'
}
let ids = tg --url $local.url run $path | from json
for id in $ids {
	success (tg --url $remote.url process get $id | complete) "the process should be located on the remote"
}
job kill $proxy
