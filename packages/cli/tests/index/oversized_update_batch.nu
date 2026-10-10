use ../lib/test.nu *

# FoundationDB must split an update batch that exceeds its transaction limit.
skip_if_no_cloud
let local = server spawn --cloud --config {
	advanced: { checkpoints: true },
	indexer: { cleaning: false },
	tracing: { stderr_format: 'json' },
}
let watch = tg checkpoint watch indexer.update.storage_and_metadata.batch | from json | get watch
tg checkpoint wait indexer.update.storage_and_metadata.batch $watch 0 | ignore

# Distinct children make the queued updates exceed the transaction's read conflict limit.
for directory in 0..<256 {
	let entries = 0..<512 | each { |child|
		let hash = ($directory * 512 + $child) | into string | fill --alignment right --character '0' --width 51
		{ name: ($child | into string), id: $'fil_01($hash)0' }
	} | transpose --header-row --as-record
	{ entries: $entries } | to json --raw | tg object put --no-tokens --bytes --kind directory | ignore
}

tg checkpoint unwatch indexer.update.storage_and_metadata.batch $watch
let output = timeout 30 tg index | complete
success $output 'the oversized update batch must drain'
