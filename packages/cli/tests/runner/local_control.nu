use ../lib/test.nu *

# Connected and standalone runtime operations on processes in a runner succeed.

let root_token = random chars
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			for (const connected of [false, true]) {
				let spawn = tg.spawn`read line; printf "%s\\n" "$line"; printf stderr >&2`
					.stdin("pipe").stdout("pipe").stderr("pipe").sandbox();
				if (connected) spawn = spawn.connection("run");
				const child = await spawn;
				const text = "x".repeat(5 * 1024 * 1024) + "\n";
				const [, stdout, stderr, outcome] = await Promise.all([
					child.stdin.writeAll(tg.encoding.utf8.encode(text)),
					child.stdout.text(),
					child.stderr.text(),
					child.wait(),
				]);
				tg.assert(stdout === text && stderr === "stderr" && outcome.exit === 0);
				let outputSpawn = tg.spawn`printf contents > "$TANGRAM_OUTPUT"`.stdio("null").sandbox();
				if (connected) outputSpawn = outputSpawn.connection("run");
				const output = tg.File.expect(await (await outputSpawn).output());
				tg.assert(await output.text === "contents");
				for (const operation of ["signal", "tty", "cancel", "disconnect", "dispose"]) {
					let spawn = tg.spawn`read line`.stdin("pipe").stdout("null").stderr("null").sandbox();
					if (operation === "tty") spawn = spawn.stdio("tty").tty({ size: { cols: 80, rows: 24 } });
					if (connected) spawn = spawn.connection("run");
					const child = await spawn;
					if (operation === "signal") {
						await child.signal(tg.Process.Signal.TERM);
						tg.assert((await child.wait()).exit === 143);
					} else if (operation === "tty") {
						await child.setTtySize({ cols: 120, rows: 40 });
						await child.cancel();
						tg.assert((await child.wait()).exit === 1);
					} else {
						await child.detach();
						const process = await tg.Process.connect(child.id, {
							lease: child.lease, location: child.location, tokens: child.tokens,
						});
						tg.assert(tg.Location.Arg.toDataString(process.location) === "remote");
						if (operation === "cancel") await process.cancel();
						else if (operation === "disconnect") process.connection.close();
						else await process[Symbol.asyncDispose]();
						tg.assert((await child.wait()).exit === 1);
					}
				}
			}
			return "ok";
		}
	',
}
let output = timeout 45s tg --url $local.url build --remote $path | complete
success $output
assert equal ($output.stdout | from json) "ok"
