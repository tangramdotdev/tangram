use ../../lib/test.nu *

skip_test --cloud

# A get that lists a region before a remote is served by the region without the remote when the region has the entry.

let remote = server spawn --name remote
let path = artifact { tangram.ts: 'export default () => "done";' }
let process = tg --url $remote.url build --no-tokens --detach $path | referent node
tg --url $remote.url wait $process | ignore
let sandbox = tg --url $remote.url process get $process | from json | get sandbox | split row '?' | first
tg --url $remote.url wait $sandbox | ignore
tg --url $remote.url index

# Pull the process and its sandbox into region b.
let instance = instance --cloud --primary-region a --regions [{ name: a }, { name: b }] --config {
	checkouts: false,
	roles: [api indexer],
}
let region_a = server spawn --instance $instance --region a --name region-a --url (instance region url $instance a)
let region_b = server spawn --instance $instance --region b --name region-b --url (instance region url $instance b)
tg --url $region_b.url remote put default $remote.url
tg --url $region_b.url pull $process
tg --url $region_b.url pull $sandbox
tg --url $region_b.url index

# Stop the remote, and get each entry through region a.
server stop $remote
let output = timeout 20s tg --url $region_a.url get --source=index --location='local(b),remote' $process | from json
assert equal $output.status finished
assert equal $output.output.options.location 'local(b)'
let output = timeout 20s tg --url $region_a.url get --source=index --location='local(b),remote' $sandbox | from json
assert equal $output.data.id $sandbox
assert equal $output.location 'local(b)'
