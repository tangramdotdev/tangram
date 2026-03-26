use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let host = tg.process.env.TANGRAM_HOST;
			tg.assert(typeof host === "string");

			let command = await tg.command({
				args: [
					"-c",
					`
						read line
						echo "stdout:$line"
						echo "stderr:$line" 1>&2
					`,
				],
				executable: "sh",
				host,
			});
			let commandId = await command.store();
			let Process = tg.Process as any;
			let process: tg.Process = await Process.spawn({
				checksum: undefined,
				command: { item: commandId, options: {} },
				create: false,
				mounts: [],
				network: false,
				parent: undefined,
				remote: undefined,
				retry: false,
				sandbox: false,
				stderr: "pipe",
				stdin: "pipe",
				stdout: "pipe",
				tty: undefined,
			});

			let stdout = "";
			let stderr = "";
			let iterator = await process.readStdio({ streams: ["stdout", "stderr"] });
			tg.assert(iterator !== undefined);
			let reader = (async () => {
				for await (let event of iterator) {
					if (event.kind === "end") {
						break;
					}
					let string = tg.encoding.utf8.decode(event.value.bytes);
					if (event.value.stream === "stdout") {
						stdout += string;
					} else {
						stderr += string;
					}
				}
			})();
			let writer = process.writeStdio(
				{ streams: ["stdin"] },
				(async function* (): AsyncIterableIterator<tg.Process.Stdio.Read.Event> {
					yield {
						kind: "chunk",
						value: {
							bytes: tg.encoding.utf8.encode("hello" + String.fromCharCode(10)),
							stream: "stdin",
						},
					};
					yield { kind: "end" };
				})(),
			);
			await Promise.all([reader, writer]);
			let wait = await process.wait();
			return { exit: wait.exit, stderr, stdout };
		}
	',
}

let output = tg run $path | from json
assert ($output == { exit: 0, stderr: "stderr:hello\n", stdout: "stdout:hello\n" })
