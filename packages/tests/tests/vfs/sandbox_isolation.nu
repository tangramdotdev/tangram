use ../../util/test.nu *
use ../../util/vfs.nu

vfs skip_unless_supported

# Concurrent sandboxes can read their own inputs without seeing each other's inputs through the VFS.
let local = server spawn --config { vfs: true }
let module = artifact {
	tangram.ts: '
		export default async () => {
			const files = [await tg.file("left\n"), await tg.file("right\n")];
			await Promise.all(files.map(file => file.store()));
			await Promise.all(files.map((file, index) => tg.run({
				args: ["-ec", `
					IFS= read -r value < "$1"
					test "$value" = "$3"
					path="$1"
					! test -e "\${path%/*}/$2"
				`, "sh", tg`${file}`, files[1 - index].id, index === 0 ? "left" : "right"],
				executable: "/bin/sh",
				host: tg.host.current,
			}).sandbox(true)));
			return true;
		};
	',
}
let output = tg build $module | from json
assert equal $output true
