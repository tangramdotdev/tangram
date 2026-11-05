use ../../util/test.nu *
use ../../util/vfs.nu

vfs skip_unless_supported

# A sandbox can enumerate an input directory through the VFS.
let local = server spawn --config { vfs: true }
let module = artifact {
	tangram.ts: '
		export default async () => {
			const directory = await tg.directory({ input: tg.file("contents") });
			return tg.command({
				args: ["-ec", "for path in \"$1\"/*; do printf \"%s\\n\" \"${path##*/}\"; done", "sh", tg`${directory}`],
				executable: "/bin/sh",
				host: tg.host.current,
			});
		};
	'
}
let command = tg build --no-tokens $module | referent node
let output = tg run --sandbox $command | complete
success $output 'a sandbox should enumerate its input directory'
assert equal ($output.stdout | str trim) input
