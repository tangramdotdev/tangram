use ../../test.nu *

let server = spawn

def run [source: string] {
	let path = artifact { tangram.ts: $source }
	let output = tg run $path | complete
	failure $output
	$output.stderr
}

let stdin = run '
	export default async function () {
		let process = await tg
			.spawn`true`
			.stdio("null");
		let input = tg.encoding.utf8.encode("hello");
		await process.stdin.write(input);
	}
'
assert ($stdin | str contains "stdin is not available")

let stdout = run '
	export default async function () {
		let process = await tg
			.spawn`true`
			.stdio("null");
		await process.stdout.read();
	}
'
assert ($stdout | str contains "stdout is not available")

let stderr = run '
	export default async function () {
		let process = await tg
			.spawn`true`
			.stdio("null");
		await process.stderr.read();
	}
'
assert ($stderr | str contains "stderr is not available")
