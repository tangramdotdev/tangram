use ../../util/test.nu *

# A process spawned with a retained connection and piped stdio round trips its stdio and can be waited repeatedly.
let local = server spawn
let path = artifact {
	tangram.ts: '
		export default async function () {
			let process =
				await tg.spawn`read line; echo "out:$line"; echo "err:$line" >&2`
					.stdio("pipe")
					.sandbox()
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
			tg.assert((await process.wait()).exit === 0);
			return "ok";
		}
	'
}
let output = tg run $path | from json
assert ($output == "ok")

let output = tg run --sandbox $path | from json
assert ($output == "ok")
