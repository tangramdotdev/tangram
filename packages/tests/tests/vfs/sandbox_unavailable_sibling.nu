use ../../util/test.nu *
use ../../util/vfs.nu

vfs skip_unless_supported

# A sandbox reads an entry of an input directory through the VFS while a sibling that it does not read is unavailable.
let source = server spawn --name source
let missing = tg --url $source.url put --no-tokens 'tg.file("missing\n")' | referent node
let local = server spawn --name local --config { vfs: true }
let directory = tg put --no-tokens ('tg.directory({ "missing": ' + $missing + ', "present": tg.file("hello\n") })') | referent node
let module = artifact {
	tangram.ts: '
		export default (directory: tg.Directory) => tg.command({
			args: ["-c", "IFS= read -r value < \"$1/present\"; test \"$value\" = hello", "sh", tg`${directory}`],
			executable: "/bin/sh",
			host: tg.host.current,
		});
	',
}
let command = tg build $module --arg-value $directory
let output = tg run --sandbox $command | complete
success $output 'the sandbox should read the available entry'
