use ../../lib/test.nu *

# Every CLI process referent carries a parent capability, including cached and remote results.
let remote = server spawn --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } },
}
let path = artifact { tangram.ts: 'export default () => "hello";' }

for location in [--local --remote] {
	for operation in [spawn build run] {
		let detach = if $operation == spawn { [] } else { [--detach] }
		let referent = tg $operation $location --sandbox --stdin=null --stdout=log --stderr=log ...$detach $path | str trim
		let tokens = $referent | referent tokens (if $location == --local { 'local' } else { 'remote' })
		assert ($tokens | is-not-empty) "the process referent must include an authorization token"
		let body = $tokens.0 | token body
		assert equal $body.resource ($referent | referent node)
		assert equal $body.permissions [process_parent]
		tg wait $referent | ignore
	}

	for operation in [build run] {
		let output = tg --quiet=false $operation $location --sandbox --stdin=null --stdout=log --stderr=log $path | complete
		success $output
		let referent = $output.stderr | lines | where { str starts-with 'info pcs_' } | first | split row ' ' | get 1
		let tokens = $referent | referent tokens (if $location == --local { 'local' } else { 'remote' })
		assert ($tokens | is-not-empty) "the progress message must include an authorization token"
		let body = $tokens.0 | token body
		assert equal $body.resource ($referent | referent node)
		assert equal $body.permissions [process_parent]

		let output = tg --quiet=false $operation $location --sandbox --stdin=null --stdout=log --stderr=log --no-tokens $path | complete
		success $output
		let referent = $output.stderr | lines | where { str starts-with 'info pcs_' } | first | split row ' ' | get 1
		assert ($referent | referent tokens local | is-empty) "the no-tokens flag must omit local capabilities"
		assert ($referent | referent tokens remote | is-empty) "the no-tokens flag must omit remote capabilities"
	}
}
