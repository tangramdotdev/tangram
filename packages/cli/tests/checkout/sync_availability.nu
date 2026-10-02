use ../lib/test.nu *

# Local checkout verification waits for an incoming sync without a default remote or a pull.
for internal in [true false] {
	let root_token = random chars
	let store = { object_concurrency: 8, object_max_batch: 1 }
	let destination = server spawn --name $'destination-($internal)' --config {
		advanced: { checkpoints: true },
		authentication: { root: { token: $root_token } },
		sync: { get: { store: { lmdb: $store, memory: $store, scylla: $store } } },
		vfs: false,
	}
	let source = server spawn --name $'source-($internal)' --config {
		remotes: { default: { token: $root_token, url: $destination.url } },
	}
	let value = $'checkout-($internal)'
	let directory = tg --url $source.url put --no-tokens ('tg.directory({ "file": tg.file(' + ($value | to json --raw) + ') })') | referent node
	let blob = tg --url $source.url put --no-tokens ('tg.blob(' + ($value | to json --raw) + ')') | referent node
	let watch = tg --url $destination.url --token $root_token checkpoint watch sync.get.store.object --params ({ id: $blob } | to json --raw) | from json | get watch
	let request_watch = tg --url $destination.url --token $root_token checkpoint watch sync.control.request --params ({ node: $directory } | to json --raw) | from json | get watch
	let log = $env.TMPDIR | path join $'push-($internal).log'
	let push = job spawn {
		let job_id = job id
		let output = tg --no-quiet --url $source.url push $directory o+e>| tee { save --force $log } | complete
		$output | job send --tag $job_id 0
	}
	timeout 10s tg --url $destination.url --token $root_token checkpoint wait sync.get.store.object $watch 0 | ignore
	wait_until { (open --raw $log) =~ 'tokens\[remote\][^\r\n]*\r?\n' } 'the push should provide an authorization token for the incoming sync'
	let referent = open --raw $log | lines | where {|line| $line =~ 'tokens\[remote\]' } | first | str trim | str replace --regex '^info ' '' | str replace --all 'tokens[remote]' 'tokens[local]'
	let path = $env.TMPDIR | path join $'checkout-($internal)'
	let checkout = job spawn {
		let job_id = job id
		let output = if $internal {
			tg --url $destination.url --token $root_token checkout $referent | complete
		} else {
			tg --url $destination.url --token $root_token checkout $referent --path $path | complete
		}
		$output | job send --tag $job_id 0
	}
	timeout 10s tg --url $destination.url --token $root_token checkpoint wait sync.control.request $request_watch 0 | ignore
	tg --url $destination.url --token $root_token checkpoint unwatch sync.control.request $request_watch
	let premature = try { job recv --tag $checkout --timeout 1sec } catch { null }
	assert equal $premature null 'checkout must wait for the stored subtree'
	tg --url $destination.url --token $root_token checkpoint unwatch sync.get.store.object $watch
	success (job recv --tag $push --timeout 10sec) 'the push should finish'
	let output = job recv --tag $checkout --timeout 10sec
	success $output 'checkout should finish when the subtree arrives'
	let path = $output.stdout | str trim
	assert equal (open --raw ($path | path join file)) $value
}
