use ../../test.nu *

const js_path = path self '../../../js'

# A request timeout returns a serialized Tangram error rather than an empty body.

let server = server spawn --config {
	advanced: {
		checkpoints: true,
	},
}

let file = artifact 'test'
let id = tg checkin $file
tg tag put --public test $id

let watch = tg checkpoint watch tag.get.read | from json | get watch
cd $js_path
let request = job spawn {
	let job_id = job id
	let output = node --input-type=module -e '
		import * as tg from "@tangramdotdev/client";

		const env = Object.fromEntries(
			Object.entries(process.env).filter(([, value]) => value !== undefined),
		);
		const decoder = new TextDecoder();
		const encoder = new TextEncoder();
		tg.setEncoding({
			utf8: {
				decode: (value) => decoder.decode(value),
				encode: (value) => encoder.encode(value),
			},
		});
		tg.setProcess({ args: [], cwd: process.cwd(), env, executable: process.execPath });
		const response = await tg.client.send(new tg.Request({
			headers: { accept: "application/json" },
			method: "GET",
			uri: "/tags/test",
		}));
		if (response.status !== 408) {
			throw new Error(`expected status 408, received ${response.status}`);
		}
		const error = tg.Error.fromData(await response.json());
		process.stdout.write((await error.message) ?? "");
		process.exit(0);
	' | complete
	$output | job send --tag $job_id 0
}

tg checkpoint wait tag.get.read $watch 0 | ignore
let output = job recv --tag $request --timeout 70sec
tg checkpoint unwatch tag.get.read $watch

success $output
snapshot $output.stdout 'the request timed out'
snapshot $output.stderr ''
