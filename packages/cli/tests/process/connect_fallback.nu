use ../lib/test.nu *

const proxy_path = path self ../lib/process_connect_proxy.mjs
let remote = server spawn --name remote
let port = port
let log = mktemp
let ready = mktemp -d | path join ready
let socket = $remote.directory | path join socket
let proxy = job spawn { node $proxy_path $socket $port $log $ready }
wait_until { $ready | path exists }
let local = server spawn --name local --config { remotes: { default: { url: $'http://127.0.0.1:($port)' } } }
let path = artifact {
	tangram.ts: '
		export default async function () {
			let ids = [];
			for (let mode of ["spawn", "run"] as const) {
				let process =
					await tg.spawn`read line; echo "out:$line"; echo "err:$line" >&2`
						.stdio("pipe")
						.sandbox()
						.location({ components: [{ name: "default" }] })
						.connection(mode);
				await process.detach();
				tg.assert(process.connection === null);
				let [, stdout, stderr, wait] = await Promise.all([
					process.stdin.writeAll(tg.encoding.utf8.encode("hello\n")),
					process.stdout.text(),
					process.stderr.text(),
					process.wait(),
				]);
				tg.assert(stdout === "out:hello\n");
				tg.assert(stderr === "err:hello\n");
				tg.assert(wait.exit === 0);
				await process.signal(tg.Process.Signal.HUP);
				ids.push(process.id);
			}
			return ids;
		}
	'
}
let ids = tg --url $local.url run $path | from json
let requests = open --raw $log | lines | each { split row '?' | first }
assert equal ($requests | where { $in == '/processes/connect' } | length) 2
for id in $ids {
	for operation in ['signal' 'stdio/read' 'stdio/write' 'wait'] {
		assert ($'/processes/($id)/($operation)' in $requests)
	}
}
job kill $proxy
