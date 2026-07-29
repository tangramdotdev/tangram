use ../../test.nu *

# An artifact produced by a build is readable through the mount when a sandboxed process references it.

if $nu.os-info.name != 'linux' {
	return
}

let server = spawn --busybox --config { vfs: true }

let built = tg build (artifact {
	tangram.ts: '
		export default () => tg.directory({ "output.txt": tg.file("build output") })
	'
}) | str trim

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default async function (built: string) {
			let dir = tg.Directory.withId(built);
			return await tg.run`cat ${dir}/output.txt`
				.env(tg.build(busybox))
				.sandbox();
		}
	',
}

let output = tg run $path --arg-string $built | str trim
assert ($output == 'build output') $'expected the build output to be readable, got: ($output)'
