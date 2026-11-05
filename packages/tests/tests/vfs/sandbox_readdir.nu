use ../../util/test.nu *
use ../../util/vfs.nu

vfs skip_unless_supported

# A sandbox can repeatedly enumerate an input directory whose entries span several VFS pages.
let local = server spawn --config { vfs: true }
let module = artifact {
	tangram.ts: '
		export default async () => {
			const file = await tg.file("contents\n");
			const names = Array.from({ length: 300 }, (_, index) => `entry_${index.toString().padStart(4, "0")}_${"x".repeat(96)}`);
			const directory = await tg.directory(Object.fromEntries(names.map(name => [name, file])));
			return tg.command({
				args: ["-ec", "for iteration in 1 2 3; do for path in \"$1\"/*; do IFS= read -r value < \"$path\"; test \"$value\" = contents; printf \"%s\\n\" \"${path##*/}\"; done; done", "sh", tg`${directory}`],
				executable: "/bin/sh",
				host: tg.host.current,
			});
		};
	'
}
let command = tg build --no-tokens $module | referent node
let output = tg run --sandbox $command | complete
success $output 'a sandbox should repeatedly enumerate its input directory'
let suffix = 'x' | fill --alignment left --character x --width 96
let expected = 0..<300 | each { |index| $'entry_($index | fill --alignment right --character 0 --width 4)_($suffix)' }
assert equal ($output.stdout | lines | chunks 300) [$expected $expected $expected]
