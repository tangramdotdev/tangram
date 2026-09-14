use ../../test.nu *

# A remote connection can select another region, and an existing process can be discovered there.
const proxy_path = path self ../lib/process_connect_proxy.mjs
let port = port
let log = mktemp
let ready = mktemp -d | path join ready
let instance = instance --primary-region west --regions [
	{ name: west }
	{ name: east, url: $'http://127.0.0.1:($port)' }
]
let east = server spawn --instance $instance --region east --name east --directory (mktemp -d)
let socket = $east.directory | path join socket
let proxy = job spawn { node $proxy_path $socket $port $log $ready }
wait_until { $ready | path exists }
let west = server spawn --instance $instance --region west --name west --directory (mktemp -d) --url (instance region url $instance west)
let missing = server spawn --name missing
let local = server spawn --name local --config {
	remotes: { default: { url: $west.url }, missing: { url: $missing.url } }
}
let path = artifact {
	tangram.ts: '
		export default async function () {
			let location = { components: [{ name: "default", regions: ["east"] }] };
			let spawned = await tg.spawn`read line; echo "$line"`
				.stdin("pipe")
				.stdout("pipe")
				.stderr("null")
				.sandbox()
				.location(location)
				.connection("run");
			tg.assert(JSON.stringify(spawned.location) === JSON.stringify(location));
			await spawned.detach();
			tg.assert(typeof spawned.id === "string");
			for (let location of [
				undefined,
				{
					components: [
						{ name: "missing" },
						{ name: "default", regions: ["west", "east"] },
					],
				},
			]) {
				let attached = await tg.Process.connect(spawned.id, {
					lease: spawned.lease,
					location,
					tokens: spawned.tokens,
				});
				tg.assert(
					JSON.stringify(attached.location) === JSON.stringify(spawned.location),
				);
				await attached.detach();
			}
			let attached = await tg.Process.connect(spawned.id, {
				lease: spawned.lease,
				location: spawned.location,
				tokens: spawned.tokens,
				reads: [{ streams: ["stdout"] }],
			});
			let [, text, wait] = await Promise.all([
				attached.stdin.writeAll(tg.encoding.utf8.encode("attached\n")),
				attached.stdout.text(),
				attached.wait(),
			]);
			tg.assert(text === "attached\n");
			tg.assert(wait.exit === 0);
			return "ok";
		}
	'
}
let output = tg --url $local.url run $path | from json
assert equal $output "ok"
assert equal (open --raw $log | lines) ['/processes/connect' '/processes/connect' '/processes/connect' '/processes/connect']
job kill $proxy
