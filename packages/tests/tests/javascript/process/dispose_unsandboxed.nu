use ../../../lib/test.nu *

# Disposing an unsandboxed process handle cancels its owned child.

let local = server spawn

let temp = mktemp --directory
let fifo = $temp | path join fifo
mkfifo $fifo

let path = artifact {
	tangram.ts: '
		export default async function (fifo: string) {
			let detached: tg.Process | undefined;
			{
				await using process = await tg.spawn({
					args: ["-c", "read line < \"$1\"", "sh", fifo],
					executable: "sh",
					stderr: "null",
					stdin: "null",
					stdout: "null",
				});
				await process.detach();
				detached = process;
			}
			tg.assert(detached !== undefined);
			await tg.run({
				args: ["-c", "echo > \"$1\"", "sh", fifo],
				executable: "sh",
				stderr: "null",
				stdin: "null",
				stdout: "null",
			});
			tg.assert((await detached.wait()).exit === 0);
			let pid: number | tg.Process.Id | undefined;
			{
				await using process = await tg.spawn({
					args: ["60"],
					executable: "sleep",
					stderr: "null",
					stdin: "null",
					stdout: "null",
				});
				pid = process.id;
			}
			tg.assert(typeof pid === "number");
			try {
				await tg.host.signal(pid, "KILL");
				return true;
			} catch {
				return false;
			}
		}
	',
}

let output = tg run $path --arg-string $fifo | from json
assert equal $output false "a disposed owned child should not continue running"
