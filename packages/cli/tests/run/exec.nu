use ../lib/test.nu *

# The tg exec, tg process exec, and tg.exec commands run a command unsandboxed and replace the current process, and exec rejects the sandbox and piped-stdio options.

let local = server spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			console.log("cli-exec");
		}
	',
}

let output = tg exec $path | complete
success $output
snapshot --normalize --redact $path (($output.stdout | str trim)) 'cli-exec'

let output = tg process exec $path | complete
success $output
snapshot --normalize --redact $path (($output.stdout | str trim)) 'cli-exec'

let javascript_path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.exec("echo javascript-client-exec");
			console.log("unreachable");
		}
	',
}

let output = tg run $javascript_path | complete
success $output
snapshot --normalize --redact $path (($output.stdout | str trim)) 'javascript-client-exec'

let js_output_path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.exec(inner);
			console.log("unreachable");
		}

		export function inner() {
			return {
				message: "javascript-client-exec-output",
			};
		}
	',
}

let output = tg run $js_output_path | from json
assert ($output == { message: "javascript-client-exec-output" })

let output = tg exec --sandbox $path | complete
failure $output
snapshot --normalize --redact $path $output.stderr '
	error an error occurred
	-> an exec must not be sandboxed

'

let output = tg exec --stdout pipe $path | complete
failure $output
snapshot --normalize --redact $path $output.stderr '
	error an error occurred
	-> stdio must be inherit or null for an exec
	   stream = stdout

'
