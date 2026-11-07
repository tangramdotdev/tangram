use ../../util/test.nu *

# Piped output remains readable after an unsandboxed child exits.

let local = server spawn
let path = artifact {
	tangram.ts: '
		export default async () => {
			const process = await tg.spawn`printf stdout; printf stderr >&2`
				.stdout("pipe")
				.stderr("pipe");
			await process.wait();
			return {
				stdout: await process.stdout.readAllToString(),
				stderr: await process.stderr.readAllToString(),
			};
		};
		export const dispose = async () => {
			const process = await tg.spawn`printf unread`.stdout("pipe");
			await process.wait();
			await process[Symbol.asyncDispose]();
			let closed = false;
			try { await process.stdout.read(); } catch { closed = true; }
			tg.assert(closed, "disposing a process must close its unread output");
			return "closed";
		};
	'
}
let output = tg run $path | from json
assert equal $output { stdout: stdout, stderr: stderr }

# Explicit disposal closes output that the caller has not consumed.
let output = tg run $'($path)#dispose' | from json
assert equal $output closed
