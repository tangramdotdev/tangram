use ../../test.nu *

# Pulling through a secondary region writes database nodes in the primary region while keeping
# objects, processes, and sandboxes in the secondary region.

let source = spawn --name source --config { advanced: { checkpoints: true } }
let path = artifact {
	tangram.ts: 'export default function () { return tg.file("regional output"); }'
}
let process = tg --url $source.url build --detach $path | str trim
let result = tg --url $source.url wait $process | from json
let output = $result.output.value | split row '?' | first
let sandbox = tg --url $source.url process get $process | from json | get sandbox
tg --url $source.url wait $sandbox | ignore
tg --url $source.url index
tg --url $source.url tag put -p routed/process $process
let tag = tg --url $source.url tag get routed/process | from json

let database_directory = mktemp -d
let database_path = $database_directory | path join 'database'
let primary_directory = mktemp -d
let secondary_directory = mktemp -d
let primary_url = $'http+unix://($primary_directory | url encode --all)%2Fsocket'
let secondary_url = $'http+unix://($secondary_directory | url encode --all)%2Fsocket'
let regions = [
	{ name: 'primary', url: $primary_url },
	{ name: 'secondary', url: $secondary_url },
]
let common = {
	advanced: {
		checkpoints: true,
		single_directory: false,
		single_process: false,
	},
	checkouts: false,
	database: { kind: 'sqlite', path: $database_path },
	primary_region: 'primary',
	regions: $regions,
}
let primary = spawn --preserve-keys --name primary --directory $primary_directory --url $primary_url --config ($common | merge { region: 'primary' })
let secondary = spawn --preserve-keys --name secondary --directory $secondary_directory --url $secondary_url --config ($common | merge { region: 'secondary' })
tg --url $secondary.url remote put default $source.url
tg --url $secondary.url pull $sandbox

let source_watch = (
	tg --url $source.url checkpoint watch sync.put.database.node.send --params ({ id: $tag.id } | to json)
	| from json
	| get watch
)
let secondary_watch = (
	tg --url $secondary.url checkpoint watch sync.get.input.node.ancestor --params ({ id: $tag.id } | to json)
	| from json
	| get watch
)
let primary_response_watch = (
	tg --url $primary.url checkpoint watch sync.request.response
	| from json
	| get watch
)
let primary_watch = (
	tg --url $primary.url checkpoint watch sync.get.input.node.ancestor --params ({ id: $tag.id } | to json)
	| from json
	| get watch
)
let pull = job spawn {
	let job_id = job id
	let output = tg --url $secondary.url pull --group-children --process-outputs routed | complete
	$output | job send --tag $job_id 0
}
tg --url $source.url checkpoint wait sync.put.database.node.send $source_watch 0 | ignore
tg --url $source.url checkpoint continue sync.put.database.node.send $source_watch 0
tg --url $source.url checkpoint unwatch sync.put.database.node.send $source_watch
tg --url $secondary.url checkpoint wait sync.get.input.node.ancestor $secondary_watch 0 | ignore
tg --url $secondary.url checkpoint continue sync.get.input.node.ancestor $secondary_watch 0
tg --url $secondary.url checkpoint unwatch sync.get.input.node.ancestor $secondary_watch
tg --url $primary.url checkpoint wait sync.request.response $primary_response_watch 0 | ignore
tg --url $primary.url checkpoint wait sync.get.input.node.ancestor $primary_watch 0 | ignore
tg --url $primary.url checkpoint continue sync.get.input.node.ancestor $primary_watch 0
tg --url $primary.url checkpoint unwatch sync.get.input.node.ancestor $primary_watch
tg --url $primary.url checkpoint continue sync.request.response $primary_response_watch 0
tg --url $primary.url checkpoint unwatch sync.request.response $primary_response_watch
success (job recv --tag $pull)

# The primary region records the tag and its permissions from the secondary region's token.
let primary_tag = tg --url $primary.url tag get routed/process | from json
assert equal $primary_tag.id $tag.id
assert equal $primary_tag.target.id $process
assert (
	$primary_tag.permissions
	| any {|permission| $permission == 'process_node_output' or $permission == 'process_subtree_output' }
) "the forwarded tag should retain permission to its process output"

# The process graph remains in the secondary region.
success (tg --url $secondary.url sandbox get --location='local(secondary)' $sandbox | complete)
failure (tg --url $secondary.url sandbox get --location='local(primary)' $sandbox | complete)
success (tg --url $secondary.url process get --location='local(secondary)' $process | complete)
failure (tg --url $secondary.url process get --location='local(primary)' $process | complete)
success (tg --url $secondary.url object get --bytes --location='local(secondary)' $output | complete)
failure (tg --url $secondary.url object get --bytes --location='local(primary)' $output | complete)
