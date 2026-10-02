use ../lib/test.nu *

# A slow remote must not hold up another remote's successful sandbox read.
let remote_slow = server spawn --name remote-slow --config { control: { read_timeout: 60 } }
let local_owner = server spawn --name local-owner
let local_client = server spawn --name local-client --config {
	remotes: { alpha: { url: $remote_slow.url }, zeta: { url: $local_owner.url } },
}
let sandbox = tg --url $local_owner.url sandbox create --no-tokens | referent node
for source in [auto runner index] {
	let output = timeout 5s tg --url $local_client.url sandbox get --remote=alpha,zeta --source $source $sandbox | complete
	success $output
	assert equal ($output.stdout | from json | get data.id) $sandbox
}
tg --url $local_owner.url sandbox destroy $sandbox
