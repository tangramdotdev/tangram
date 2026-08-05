use ../../test.nu *

# Cleaning a process deletes it from the index but leaves its grants, so a grant update enqueued for the process afterward finds no process. The indexer must tolerate that. Failing the update instead leaves the entry at the head of the update queue, which blocks every later update and logs the failure on every retry, including after a restart.

let server = spawn --config { tracing: { stderr_format: 'json' } }

let path = artifact {
	tangram.ts: '
		export default () => tg.file("hello");
	'
}

# Build with a public grant, so the index has a grant on the process.
let build = tg build --detach --verbose --public $path | from json
let process = $build.process
tg wait $process

# Clean deletes the process from the index and leaves its grants.
tg clean

# Revoking the grant enqueues a grant update for the deleted process.
tg grants delete public subtree $process

# The update queue must drain.
let index = timeout 15 tg index | complete
success $index "the update queue must drain after a grant update for a cleaned process"

let errors = server_errors $server | where { $in | str starts-with 'tangram_server::indexer' }
snapshot $errors ''
