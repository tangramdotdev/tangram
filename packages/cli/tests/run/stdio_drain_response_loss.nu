use ../../test.nu *

# Reading a sandboxed child's piped stdout captures all of it even when the child
# emits output in bursts slower than the stdio_drain_timeout. That is the corner case
# where each drain read blocks past the timeout, so its reply must survive the retry
# loop's subscription gap rather than being lost.

let server = spawn --config { runner: { stdio_drain_timeout: 0.01 } }

let path = artifact {
	tangram.ts: '
		export default async function () {
			let process = await tg.spawn(child).stdio("pipe").sandbox();
			let stdout = await process.stdout.readAllToString();
			console.log(stdout);
		}

		export async function child() {
			for (let i = 0; i < 8; i++) {
				console.log("line " + i);
				await tg.sleep(0.05);
			}
		}
	',
}

let output = tg run $path | complete
success $output
assert ($output.stdout | str contains "line 7")
