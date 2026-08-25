use ../../test.nu *

# Running a command in the current sandbox (no .sandbox()) widens the sandbox's grants to the referenced artifact.

if $nu.os-info.name != 'linux' {
	return
}

let server = server spawn --busybox --config { vfs: true }

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default async function () {
			let widened = tg.directory({ "file.txt": tg.file("widened contents") });
			return await tg.run`cat ${widened}/file.txt`.env(tg.build(busybox));
		}
	',
}

let output = tg run --sandbox $path | str trim
assert ($output == 'widened contents') $'expected the widened artifact to be readable in the current sandbox, got: ($output)'
