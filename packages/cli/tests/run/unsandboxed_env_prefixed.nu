use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			const host = tg.process.env.TANGRAM_HOST;
			tg.assert(typeof host === "string");
			return await tg.run({
				args: ["-c", "printf \"%s\\n%s\\n\" \"$TEST_ENV_INHERIT\" \"$TANGRAM_ENV_TEST_ENV_INHERIT\""],
				executable: "sh",
				host,
			}).env({
				TEST_ENV_INHERIT: 42,
			});
		}
	',
}

let output = tg run $path | lines
assert (($output | length) == 2)
assert ($output.0 == "42")
assert ($output.1 == "42")
