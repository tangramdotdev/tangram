use ../lib/test.nu *

# All process readers decode inline metadata and empty attributes that mark serialized file contents.

let local = server spawn
let path = artifact {
	tangram.ts: '
		export default async function () {
			const script = `
				printf "%s" "$1" > "$TANGRAM_OUTPUT"
				if command -v xattr >/dev/null; then
					xattr -w "$2" "$3" "$TANGRAM_OUTPUT"
				else
					value=$(printf "%s" "$3" | od -An -tx1 | tr -d " \n")
					setfattr -n "$2" -v "0x$value" "$TANGRAM_OUTPUT"
				fi
				exit "\${4:-0}"
			`;
			const large = "x".repeat(70_000);
			for (const sandbox of [false]) {
				for (const [name, payload] of [
					["output", tg.Value.print(large)],
					["error", JSON.stringify({ message: "failed" })],
					["outcome", JSON.stringify({ exit: 0, output: large })],
				]) {
					for (const contents of [false, true]) {
						const expected = contents ? large : "inline";
						const value = name === "output" ? tg.Value.print(expected) : name === "outcome" ? JSON.stringify({ exit: 0, output: expected }) : payload;
						const child = await tg.spawn({
							executable: "sh",
							args: ["-c", script, "_", contents ? value : "ignored", `user.tangram.${name}`, contents ? "" : value],
						}).sandbox(sandbox);
						const outcome = await child.wait();
						if (name === "error") {
							tg.assert(outcome.error !== null);
							tg.assert(outcome.output === undefined);
						} else {
							tg.assert(outcome.error === null && outcome.exit === 0);
							tg.assert(outcome.output === expected);
						}
					}
				}
				for (const [data, exit] of [[{ exit: 0 }, 0], [{ exit: 0, output: null }, 0], [{ exit: 7 }, 0], [{ exit: 0 }, 7]]) {
					const child = await tg.spawn({
						executable: "sh",
						args: ["-c", script, "_", JSON.stringify(data), "user.tangram.outcome", "", String(exit)],
					}).sandbox(sandbox);
					const outcome = await child.wait();
					tg.assert(outcome.exit === exit);
					tg.assert(outcome.output === data.output);
				}
			}
			for (const sandbox of [false, true]) {
				const value = await tg.run(produce).sandbox(sandbox);
				tg.assert(value === large);
			}
			return "ok";
		}

		export function produce() { return "x".repeat(70_000); }
	'
}
let output = tg run --no-sandbox $path | complete
success $output
snapshot ($output.stdout | str trim) '"ok"'

# The Rust unsandboxed reader recognizes a combined outcome directly.
let input = artifact (file --xattrs { "user.tangram.outcome": '' } '{"exit":0,"output":"contents"}')
let output = tg run --no-sandbox --executable sh -- -c 'cp -a "$1" "$TANGRAM_OUTPUT"' _ $input | complete
success $output
snapshot ($output.stdout | str trim) '"contents"'

# The real exit status takes priority over a successful serialized outcome.
let output = tg run --no-sandbox --executable sh -- -c 'cp -a "$1" "$TANGRAM_OUTPUT"; exit 7' _ $input | complete
failure $output
assert ($output.stderr | str contains 'code 7') 'the actual exit code must take priority'

let input = artifact (file --xattrs { "user.tangram.outcome": '' } '{"exit":7,"output":"contents"}')
let output = tg run --no-sandbox --executable sh -- -c 'cp -a "$1" "$TANGRAM_OUTPUT"' _ $input | complete
success $output
snapshot ($output.stdout | str trim) '"contents"'
