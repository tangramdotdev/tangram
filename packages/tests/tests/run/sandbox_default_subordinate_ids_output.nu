use ../../util/test.nu *

# A build checks in an output whose files and directories other identities of the sandbox own with modes that deny access to everyone else.

default_id_maps | ignore
let local = server spawn --busybox

let script = r#'
	set -eu
	mkdir "$TANGRAM_OUTPUT"
	chmod 1777 "$TANGRAM_OUTPUT"
	mkdir -p "$TANGRAM_OUTPUT/private/nested"
	printf secret > "$TANGRAM_OUTPUT/private/nested/file"
	chmod 600 "$TANGRAM_OUTPUT/private/nested/file"
	printf 'exit 0\n' > "$TANGRAM_OUTPUT/private/nested/script"
	chmod 700 "$TANGRAM_OUTPUT/private/nested/script"
	ln -s file "$TANGRAM_OUTPUT/private/nested/link"
	chmod 700 "$TANGRAM_OUTPUT/private/nested" "$TANGRAM_OUTPUT/private"
	chown -h -R 1:2 "$TANGRAM_OUTPUT/private"
	chpst -u 65534:65534 sh -c 'umask 077; mkdir "$1/nobody"; printf nobody > "$1/nobody/file"' sh "$TANGRAM_OUTPUT"
	chown 5:5 "$TANGRAM_OUTPUT"
'#
let command = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default (script: string) => tg.run({
			executable: "/bin/sh",
			args: ["-c", script],
		}).env(tg.build(busybox)).sandbox();
	',
}
let output = tg build $command --arg-string $script | complete
success $output 'the build should check in the output'
let path = tg checkout ($output.stdout | str trim) | str trim
assert equal (open --raw ($path | path join private nested file)) 'secret'
assert equal (open --raw ($path | path join private nested link)) 'secret'
assert equal (^stat -c %a ($path | path join private nested script) | str trim) '555'
assert equal (open --raw ($path | path join nobody file)) 'nobody'
