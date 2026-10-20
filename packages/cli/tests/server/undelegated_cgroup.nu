use ../lib/test.nu *

# A build succeeds when the server starts in a systemd scope without cgroup delegation.

if $nu.os-info.name != 'linux' or (which systemd-run | is-empty) {
	skip_test 'this test requires systemd'
}
let manager = ^systemctl --user show --property=Version | complete
if $manager.exit_code != 0 {
	skip_test 'this test requires a systemd user manager'
}

let path = artifact {
	tangram.ts: 'export default function () { return "hello"; }',
}
let directory = mktemp -d
let slice = $'tangramtest(random chars --length 16).slice'
let output = with-env { TANGRAM_MODE: auto } {
	^systemd-run --user --scope --quiet --slice $slice tg -d $directory --no-remotes build $path | complete
}
tg -d $directory server stop
^systemctl --user stop $slice | complete | ignore
success $output
snapshot ($output.stdout | str trim) '"hello"'
