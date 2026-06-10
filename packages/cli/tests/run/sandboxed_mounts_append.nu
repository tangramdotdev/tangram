use ../../test.nu *

# Successive .mount calls on a sandboxed process append the mounts in order and the resulting mount list is reported back faithfully.

let server = spawn --config {
	sandbox: {
		finalizer: false,
	},
}

let a = mktemp --directory | str trim
let b = mktemp --directory | str trim

let path = artifact {
	tangram.ts: '
		export default async function (a, b) {
			let process = await tg
				.spawn({
					executable: "sh",
					args: ["-lc", "true"],
				})
				.sandbox()
				.mount({ source: a, target: "/work/a" })
				.mount({ source: b, target: "/work/b" });
			await process.wait();
			return process.mounts;
		}
	',
}

let output = tg run $path --arg-string $a --arg-string $b | from json | each { |mount| { source: $mount.source, target: $mount.target } }
assert ($output == [
	{ source: $a, target: "/work/a" },
	{ source: $b, target: "/work/b" },
])
