use ../../test.nu *

const js_path = path self '../../../js'

# The compiled Node.js client uses the default host and connects to the server using the inherited Tangram URL.

let server = spawn
let build_output = bun run --filter @tangramdotdev/client build | complete
success $build_output

cd $js_path

let output = node --input-type=module -e '
	import assert from "node:assert/strict";
	import * as tg from "@tangramdotdev/client";

	const env = Object.fromEntries(
		Object.entries(process.env).filter(([, value]) => value !== undefined),
	);
	tg.setProcess({
		args: process.argv.slice(2),
		cwd: process.cwd(),
		env,
		executable: process.execPath,
	});

	assert.equal(
		tg.host.checksum("Hello, World!", "sha256"),
		"sha256:dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f",
	);
	assert.equal(
		tg.host.objectId({
			kind: "blob",
			value: { kind: "leaf", bytes: "SGVsbG8=" },
		}),
		"blb_01zby8hmr9wc7c8t2g8c7qt29cyt8hkeg2y6y1yahh585dx6hebf2g",
	);
	const value = { hello: "world", nested: [true, 42, null] };
	assert.deepEqual(tg.Value.parse(tg.Value.stringify(value)), value);
	const child = await tg.host.spawn({
		args: ["-c", "printf \"Hello, World!\""],
		cwd: null,
		env,
		executable: "sh",
		stderr: "null",
		stdin: "null",
		stdout: "pipe",
	});
	assert.notEqual(child.stdout, null);
	const bytes = await tg.host.read(child.stdout);
	assert.equal(new TextDecoder().decode(bytes), "Hello, World!");
	assert.equal(await tg.host.read(child.stdout), null);
	await tg.host.close(child.stdout);
	assert.deepEqual(await tg.host.wait(child.pid), { exit: 0 });

	const response = await tg.client.send(
		new tg.Request({ method: "GET", uri: "/health" }),
	);
	assert.equal(response.status, 200);
	const body = JSON.parse(new TextDecoder().decode(await response.collect()));
	assert.equal(typeof body.version, "string");
	assert.ok(body.version.length > 0);
	process.stdout.write("hello from the Node.js client");
	process.exit(0);
' | complete

success $output
assert equal ($output.stdout | str trim) 'hello from the Node.js client' 'the Node.js client should receive the server health'
