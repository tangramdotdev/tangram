use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			console.log("cli-exec");
		}
	',
}

let output = tg exec $path | complete
success $output
assert (($output.stdout | str trim) == "cli-exec")

let output = tg process exec $path | complete
success $output
assert (($output.stdout | str trim) == "cli-exec")

let js_path = artifact {
	tangram.ts: '
		export default async () => {
			await tg.exec("echo js-client-exec");
			console.log("unreachable");
		}
	',
}

let output = tg run $js_path | complete
success $output
assert (($output.stdout | str trim) == "js-client-exec")
assert (not ($output.stdout | str contains "unreachable"))

let js_output_path = artifact {
	tangram.ts: '
		export default async () => {
			await tg.exec(inner);
			console.log("unreachable");
		}

		export function inner() {
			return {
				message: "js-client-exec-output",
			};
		}
	',
}

let output = tg run $js_output_path | from json
assert ($output == { message: "js-client-exec-output" })

let output = tg exec --sandbox $path | complete
failure $output
assert ($output.stderr | str contains "an exec must not be sandboxed")

let output = tg exec --stdout pipe $path | complete
failure $output
assert ($output.stderr | str contains "stdio must be inherit or null for an exec")
