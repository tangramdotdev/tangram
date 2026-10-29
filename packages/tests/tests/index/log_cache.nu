use ../../lib/test.nu *

# Live logs expire after finish, while the finished blob remains readable.
for cache in [lmdb rocksdb fjall] {
	let local = server spawn --name local --now '2026-01-01T00:00:00Z' --config {
		advanced: { checkpoints: true },
		cache: { kind: $cache },
		indexer: { log_cache: { batch_size: 1 } },
		process: { log_time_to_live: 0 },
	}
	let watch = tg checkpoint watch indexer.log_cache.delete | from json | get watch
	let path = artifact { tangram.ts: 'export default function () { console.log("stdout"); console.error("stderr"); }' }
	let process = tg build --no-tokens --detach $path | referent node
	timeout 10s tg wait $process | ignore

	# Advance the clock to wake log cache cleaning, and wait for it to delete the live log.
	advance_time $local 1min
	tg checkpoint wait --timeout 10s indexer.log_cache.delete $watch 0 | ignore
	let data = tg get --no-tokens $process | from json
	assert ($data.log? | is-not-empty)
	let output = tg log --no-timeout $process | complete
	success $output
	assert equal $output.stdout "stdout\n"
	assert equal $output.stderr "stderr\n"
	tg checkpoint unwatch indexer.log_cache.delete $watch
}
