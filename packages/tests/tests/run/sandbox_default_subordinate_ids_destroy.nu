use ../../util/test.nu *

# Destroying a sandbox whose workload left files that other identities of the sandbox own, in directories that deny access to everyone else, removes them, so that the server's directory remains removable by its user.

default_id_maps | ignore

let script = r#'
	set -eu
	for directory in /data /tmp/data; do
		mkdir -p "$directory/nested"
		printf contents > "$directory/nested/file"
		ln -s file "$directory/nested/link"
		mkfifo "$directory/nested/fifo"
		chown -h -R 1:2 "$directory"
		chmod 700 "$directory/nested" "$directory"
		chmod 000 "$directory/nested/file"
	done
	mkdir -m 1777 /shared
	chpst -u 65534:65534 sh -c 'umask 077; mkdir -p /shared/nobody/nested; printf contents > /shared/nobody/nested/file; chmod 500 /shared/nobody/nested'
	echo written
'#
let command = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default (script: string, sandbox: string) => tg.run({
			executable: "/bin/sh",
			args: ["-c", script],
		}).env(tg.build(busybox)).sandbox(sandbox);
	',
}

# Destroy a stopped sandbox, whose destruction finishes once its directory is removed.
let directory = mktemp --directory
let local = server spawn --busybox --directory $directory
let sandbox = tg sandbox create --no-tokens | referent node
let output = tg run $command --arg-string $script --arg-string $sandbox | complete
success $output 'the workload should write the files'
assert equal ($output.stdout | str trim) 'written'
tg sandbox stop --wait $sandbox
let output = tg sandbox destroy --wait $sandbox | complete
success $output 'the stopped sandbox should be destroyed'
assert equal (tg sandbox get $sandbox | from json | get data.status) destroyed

# The server's user owns every file in the server's directory once the server stops, so that it can make the read-only store writable and remove the directory.
server stop $local
let output = ^chmod -R u+rwX $directory | complete
success $output 'the server directory should be writable by its user'
let output = ^rm -rf $directory | complete
success $output 'the server directory should be removable'

# Destroy a sandbox while it runs, and then run the workload in another sandbox.
let local = server spawn --busybox
let sandbox = tg sandbox create --no-tokens | referent node
let output = tg run $command --arg-string $script --arg-string $sandbox | complete
success $output 'the workload should write the files'
let output = tg sandbox destroy --wait $sandbox | complete
success $output 'the running sandbox should be destroyed'
assert equal (tg sandbox get $sandbox | from json | get data.status) destroyed
let sandbox = tg sandbox create --no-tokens | referent node
let output = tg run $command --arg-string $script --arg-string $sandbox | complete
success $output 'another sandbox should write the files'
tg sandbox destroy --wait $sandbox
