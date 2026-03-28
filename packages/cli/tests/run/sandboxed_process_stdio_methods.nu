use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let process = await tg
				.spawn`
					read line
					echo "stdout:$line"
					echo "stderr:$line" 1>&2
				`
				.stdin("pipe")
				.stdout("pipe")
				.stderr("pipe")
				.sandbox();
			tg.assert(process.stdin !== undefined);
			tg.assert(process.stdout !== undefined);
			tg.assert(process.stderr !== undefined);
			let input = tg.encoding.utf8.encode("hello\n");
			let [written, stdout, stderr] = await Promise.all([
				process.stdin.write(input).then(async (written) => {
					await process.stdin!.close();
					return written;
				}),
				(async () => {
					let chunks = [];
					while (true) {
						let chunk = await process.stdout!.read();
						if (chunk === undefined) {
							break;
						}
						chunks.push(tg.encoding.utf8.decode(chunk));
					}
					return chunks.join("");
				})(),
				(async () => {
					let chunks = [];
					while (true) {
						let chunk = await process.stderr!.read();
						if (chunk === undefined) {
							break;
						}
						chunks.push(tg.encoding.utf8.decode(chunk));
					}
					return chunks.join("");
				})(),
			]);
			tg.assert(written === input.length);
			let wait = await process.wait();
			return { exit: wait.exit, stderr, stdout };
		}
	',
}

let output = tg run $path | from json
assert ($output == { exit: 0, stderr: "stderr:hello\n", stdout: "stdout:hello\n" })
