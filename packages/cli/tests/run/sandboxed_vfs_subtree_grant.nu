use ../../test.nu *

# A grant on a referenced directory covers its whole subtree, so a deeply nested file is readable.

if $nu.os-info.name != 'linux' {
	return
}

let server = spawn --busybox --config { vfs: true }

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default async function () {
			let data = tg.directory({
				a: tg.directory({ b: tg.directory({ "deep.txt": tg.file("nested contents") }) }),
			});
			return await tg.run`cat ${data}/a/b/deep.txt`
				.env(tg.build(busybox))
				.sandbox();
		}
	',
}

let output = tg run $path | str trim
assert ($output == 'nested contents') $'expected the nested descendant to inherit the subtree grant, got: ($output)'
