use ../lib/test.nu *

# A remote lookup cannot begin until the region lookup has returned no result.
let database_directory = mktemp -d
let database_path = $database_directory | path join database
let instance = instance --primary-region a --regions [{ name: a }, { name: b }] --config {
	advanced: { checkpoints: true },
	database: { kind: sqlite, path: $database_path },
	roles: [api indexer],
}
let region_a = server spawn --instance $instance --region a --name region-a --directory (mktemp -d) --url (instance region url $instance a)
let region_b = server spawn --instance $instance --region b --name region-b --directory (mktemp -d) --url (instance region url $instance b)
let remote = server spawn --name remote --config { advanced: { checkpoints: true } }
tg --url $region_a.url remote put default $remote.url
let path = artifact { tangram.ts: 'export default () => "done";' }
let process = tg --url $remote.url build --detach $path | str trim
tg --url $remote.url wait --source=index $process | ignore
let sandbox = tg --url $remote.url sandbox create | str trim
tg --url $remote.url index

for entry in [{ kind: process, id: $process }, { kind: sandbox, id: $sandbox }] {
	let checkpoint = $'($entry.kind).get.index'
	let region_watch = tg --url $region_b.url checkpoint watch $checkpoint | from json | get watch
	let remote_watch = tg --url $remote.url checkpoint watch $checkpoint | from json | get watch
	let reader = job spawn {
		let job_id = job id
		let output = timeout 20s tg --url $region_a.url $entry.kind get --source=index --location='local(b),remote' $entry.id | complete
		$output | job send --tag $job_id 0
	}
	timeout 10s tg --url $region_b.url checkpoint wait $checkpoint $region_watch 0 | ignore
	let remote_request = job spawn {
		let job_id = job id
		let output = timeout 15s tg --url $remote.url checkpoint wait $checkpoint $remote_watch 0 | complete
		$output | job send --tag $job_id 0
	}
	assert equal (try { job recv --tag $remote_request --timeout 300ms } catch { null }) null
	tg --url $region_b.url checkpoint unwatch $checkpoint $region_watch
	success (job recv --tag $remote_request --timeout 15sec)
	tg --url $remote.url checkpoint unwatch $checkpoint $remote_watch
	success (job recv --tag $reader --timeout 15sec)
}
tg --url $remote.url sandbox destroy $sandbox
