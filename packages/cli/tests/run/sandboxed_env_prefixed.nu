use ../../test.nu *

# An environment variable set on a sandboxed process is visible both under its plain name and under its TANGRAM_ENV_ prefixed name.

let server = spawn --busybox

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";

		export default async function () {
			const host = tg.host.current;
			tg.assert(typeof host === "string");
			return await tg.run({
				args: ["-c", "printf \"%s\\n%s\\n\" \"$TEST_ENV_INHERIT\" \"$TANGRAM_ENV_TEST_ENV_INHERIT\""],
				env: tg.build(busybox),
				executable: "sh",
				host,
			}).env({
				TEST_ENV_INHERIT: 42,
			}).sandbox();
		}
	',
}

let output = tg run $path | lines
assert (($output | length) == 2)
assert ($output.0 == "42")
assert ($output.1 == "42")
