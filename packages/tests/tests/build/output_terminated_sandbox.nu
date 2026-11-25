use ../../util/test.nu *

# A build checks in its output after its sandbox ends every process in it, and the checked-in artifact has the files, modes, and symlinks that the build wrote.

let local = server spawn --busybox
let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default () => tg.build`
			mkdir -p ${tg.output}/directory
			echo hello > ${tg.output}/directory/file
			echo "echo hi" > ${tg.output}/script
			chmod +x ${tg.output}/script
			ln -s directory/file ${tg.output}/link
		`.env(tg.build(busybox));
	'
}
let output = tg build --no-tokens $path | complete
success $output
let id = $output.stdout | str trim

# The checkout has the build's files.
let checkout = mktemp --directory | path join checkout
tg checkout $id --path $checkout
assert equal (open --raw ($checkout | path join directory file)) "hello\n"
assert equal (open --raw ($checkout | path join script)) "echo hi\n"
assert (ls -l ($checkout | path join script) | get 0.mode | str contains 'x')
assert equal (ls -l ($checkout | path join link) | get 0.target) 'directory/file'
assert equal (open --raw ($checkout | path join link)) "hello\n"
