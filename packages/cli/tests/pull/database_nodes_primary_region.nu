use ../lib/test.nu *

# Pulling through a secondary region writes database nodes in the primary region while keeping
# objects, processes, and sandboxes in the secondary region.

let local_source = server spawn --name local-source --config { advanced: { checkpoints: true } }
let path = artifact {
	tangram.ts: 'export default function () { return tg.file("regional output"); }'
}
let process = tg --url $local_source.url build --no-tokens --detach $path | referent node
let result = tg --url $local_source.url wait --no-tokens $process | from json
let output = $result.output.value
let sandbox = tg --url $local_source.url process get $process | from json | get sandbox
tg --url $local_source.url wait $sandbox | ignore
tg --url $local_source.url index
tg --url $local_source.url tag put -p routed/process $process
let tag = tg --url $local_source.url tag get routed/process | from json

let database_directory = mktemp -d
let database_path = $database_directory | path join 'database'
let primary_directory = mktemp -d
let secondary_directory = mktemp -d
let regions = [
	{ name: 'primary' },
	{ name: 'secondary' },
]
let common = {
	advanced: {
		checkpoints: true,
		single_directory: false,
		single_process: false,
	},
	checkouts: false,
	database: { kind: 'sqlite', path: $database_path },
}
let instance = instance --primary-region primary --regions $regions --config $common
let remote_primary = server spawn --instance $instance --region primary --preserve-keys --name remote-primary --directory $primary_directory --url (instance region url $instance primary)
let remote_secondary = server spawn --instance $instance --region secondary --preserve-keys --name remote-secondary --directory $secondary_directory --url (instance region url $instance secondary)
tg --url $remote_secondary.url remote put default $local_source.url
tg --url $remote_secondary.url pull $sandbox

let source_watch = (
	tg --url $local_source.url checkpoint watch sync.put.database.node.send --params ({ id: $tag.id } | to json)
	| from json
	| get watch
)
let secondary_watch = (
	tg --url $remote_secondary.url checkpoint watch sync.get.input.node.ancestor --params ({ id: $tag.id } | to json)
	| from json
	| get watch
)
let primary_response_watch = (
	tg --url $remote_primary.url checkpoint watch sync.request.response
	| from json
	| get watch
)
let primary_watch = (
	tg --url $remote_primary.url checkpoint watch sync.get.input.node.ancestor --params ({ id: $tag.id } | to json)
	| from json
	| get watch
)
let pull = job spawn {
	let job_id = job id
	let output = tg --url $remote_secondary.url pull --group-children --process-output-objects routed | complete
	$output | job send --tag $job_id 0
}
tg --url $local_source.url checkpoint wait sync.put.database.node.send $source_watch 0 | ignore
tg --url $local_source.url checkpoint continue sync.put.database.node.send $source_watch 0
tg --url $local_source.url checkpoint unwatch sync.put.database.node.send $source_watch
tg --url $remote_secondary.url checkpoint wait sync.get.input.node.ancestor $secondary_watch 0 | ignore
tg --url $remote_secondary.url checkpoint continue sync.get.input.node.ancestor $secondary_watch 0
tg --url $remote_secondary.url checkpoint unwatch sync.get.input.node.ancestor $secondary_watch
tg --url $remote_primary.url checkpoint wait sync.request.response $primary_response_watch 0 | ignore
tg --url $remote_primary.url checkpoint wait sync.get.input.node.ancestor $primary_watch 0 | ignore
tg --url $remote_primary.url checkpoint continue sync.get.input.node.ancestor $primary_watch 0
tg --url $remote_primary.url checkpoint unwatch sync.get.input.node.ancestor $primary_watch
tg --url $remote_primary.url checkpoint continue sync.request.response $primary_response_watch 0
tg --url $remote_primary.url checkpoint unwatch sync.request.response $primary_response_watch
success (job recv --tag $pull)

# The primary region records the tag and preserves its target permissions from the secondary region's token.
let primary_tag = tg --url $remote_primary.url tag get routed/process | from json
assert equal $primary_tag.id $tag.id
assert equal $primary_tag.target.id $process
let target = tg --url $remote_primary.url children --verbose $tag.id | from json | get data.0
let permissions = $target.options?.tokens?.local? | default [] | each {|token|
	$token | split row '.' | get 1 | decode base64 | decode utf-8 | from json | get permissions
} | flatten
assert (
	$permissions
	| any {|permission| $permission == 'process_node_output_objects' or $permission == 'process_subtree_output_objects' }
) "the forwarded tag should retain permission to its process output"

# The process graph remains in the secondary region.
success (tg --url $remote_secondary.url sandbox get --location='local(secondary)' $sandbox | complete)
failure (tg --url $remote_secondary.url sandbox get --location='local(primary)' $sandbox | complete)
success (tg --url $remote_secondary.url process get --location='local(secondary)' $process | complete)
failure (tg --url $remote_secondary.url process get --location='local(primary)' $process | complete)
success (tg --url $remote_secondary.url object get --bytes --location='local(secondary)' $output | complete)
failure (tg --url $remote_secondary.url object get --bytes --location='local(primary)' $output | complete)
