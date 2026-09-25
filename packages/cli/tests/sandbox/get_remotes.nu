use ../lib/test.nu *

# A slow remote must not hold up another remote's successful sandbox read.
let slow = server spawn --name slow --config { control: { read_timeout: 60 } }
let owner = server spawn --name owner
let client = server spawn --name client --config {
	remotes: { alpha: { url: $slow.url }, zeta: { url: $owner.url } },
}
let sandbox = tg --url $owner.url sandbox create | str trim
for source in [auto runner index] {
	let output = timeout 5s tg --url $client.url sandbox get --remote=alpha,zeta --source $source $sandbox | complete
	success $output
	assert equal ($output.stdout | from json | get data.id) $sandbox
}
tg --url $owner.url sandbox destroy $sandbox
