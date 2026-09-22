use ../lib/test.nu *

# A missing stdin blob fails the process, including when the child exits without reading stdin.

let server = server spawn
let path = artifact {
	tangram.ts: '
		export default async function (script: string) {
			const child = await tg.spawn({
				executable: "sh",
				args: ["-c", script],
				stdin: tg.Blob.withId("blb_01041061050r3gg28a1c60t3gf208h44rm2mb1e60s38dhr78y3wg0"),
			}).sandbox();
			const output = await child.wait();
			return { process: child.id, exit: output.exit };
		}
	'
}

for script in ["read value" "exit 0"] {
	let output = timeout 15s tg build $path -a $script | complete
	success $output
	let result = $output.stdout | from json
	assert ($result.exit != 0) "the stdin read must fail the process"
	let process = tg get $result.process | from json
	let error = tg get $process.error
	assert ($error | str contains "failed to read process stdin blob") $error
}
