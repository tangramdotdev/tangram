use ../../test.nu *

# The cleaner deletes a process while an item update for it is still queued, because the two run independently. The indexer must tolerate the missing process. Failing the update instead leaves the entry at the head of the update queue, which blocks every later update and logs the failure on every retry, including after a restart.

let server = spawn --config {
	advanced: {
		checkpoints: true,
	},
	process: {
		time_to_live: 1,
	},
	tracing: {
		stderr_format: 'json',
	},
}

let path = artifact {
	tangram.ts: '
		export default () => tg.file("hello");
	'
}

let build = tg build --detach --verbose $path | from json
let process = $build.process
tg wait $process
let data = tg process get $process

# Hold the update task so that the queue cannot drain.
let batch_watch = (
	tg checkpoint watch indexer.update.batch
	| from json
	| get watch
)
tg checkpoint wait indexer.update.batch $batch_watch 0 | ignore

# Putting the process queues an item update for it.
$data | tg process put $process

# Watch the cleaner only now, so that the hit below is necessarily a deletion that follows the queued update.
let delete_watch = (
	tg checkpoint watch cleaner.process.delete --params $'{"process": "($process)"}'
	| from json
	| get watch
)

# Wait for the cleaner to delete the process, so that the queued item update refers to a process that is gone.
tg checkpoint wait cleaner.process.delete $delete_watch 0 | ignore
tg checkpoint continue cleaner.process.delete $delete_watch 0
tg checkpoint unwatch cleaner.process.delete $delete_watch

# Release the update task.
tg checkpoint continue indexer.update.batch $batch_watch 0
tg checkpoint unwatch indexer.update.batch $batch_watch

# The update queue must drain.
let index = timeout 15 tg index | complete
success $index "the update queue must drain after an item update for a cleaned process"

let errors = server_errors $server | where { $in | str starts-with 'tangram_server::indexer' }
snapshot $errors ''
