use ../lib/test.nu *

# Long server paths must use the actual API socket as the sandbox bind source.
let directory = (mktemp -d) | path join ('runner' | fill --character x --width 160)
let server = server spawn --directory $directory --config { vfs: false }
let path = artifact {
	tangram.ts: 'export default () => tg.file("hello");',
}
let output = timeout 10 tg build $path | complete
success $output
assert equal (tg cat ($output.stdout | str trim)) hello
