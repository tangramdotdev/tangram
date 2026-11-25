use ../../util/test.nu *

# A build whose script leaves a background process running finishes when the script exits, because the process left behind is killed.

let local = server spawn --busybox
let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default () => tg.build`
			(while :; do :; done) &
			echo hello
			echo hello > ${tg.output}
		`.env(tg.build(busybox));
	'
}
let output = tg build --no-tokens $path | complete
success $output
let id = $output.stdout | str trim
assert equal (tg read $id) "hello"
