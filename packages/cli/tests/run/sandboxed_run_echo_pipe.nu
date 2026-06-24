use ../../test.nu *

# The stdout of a sandboxed spawned process with piped stdio can be read to a string and logged.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let process = await tg.spawn`echo hello`.stdio("pipe").sandbox();
			let stdout = await process.stdout.readAllToString();
			console.log(stdout);
		}
	',
}

let output = tg run $path | complete
success $output
$output.stdout | hexyl
assert equal $output.stdout "hello\n\n"
