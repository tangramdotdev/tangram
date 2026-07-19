use ../../../test.nu *

# Disposing an unsandboxed process handle cancels its owned child.

let server = spawn

let temp = mktemp --directory
let marker = $temp | path join marker

let path = artifact {
	tangram.ts: '
		export default async function (marker: string) {
			let detached: tg.Process | undefined;
			{
				await using process = await tg.spawn`sleep 0.1`
					.stderr("null")
					.stdin("null")
					.stdout("null");
				process.detach();
				detached = process;
			}
			tg.assert(detached !== undefined);
			tg.assert((await detached.wait()).exit === 0);
			{
				await using process = await tg.spawn({
					args: ["-c", "sleep 0.25; touch \"$1\"; sleep 60", "sh", marker],
					executable: "sh",
					stderr: "null",
					stdin: "null",
					stdout: "null",
				});
			}
			await tg.sleep(2);
			return await tg.host.exists(marker);
		}
	',
}

let output = tg run $path --arg-string $marker | from json
assert equal $output false "a disposed owned child should not continue running"
