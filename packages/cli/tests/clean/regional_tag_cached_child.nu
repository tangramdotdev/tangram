use ../lib/test.nu *

# A cross-region cache hit should touch the child without retaining it through a foreign parent.

# Create isolated regions with shared tag metadata and separate object stores.
let common = {
	advanced: { checkpoints: true, single_directory: true, single_process: false },
	checkouts: true,
	database: { index_queue: { wakeup_interval: 0.01 } },
	indexer: { cleaning: { poll_interval: 0.01 } },
	roles: [api indexer scheduler runner],
}
let common = if ($env.TANGRAM_TEST_CLOUD? | default '' | is-empty) {
	$common | merge deep { database: { kind: sqlite, path: ((mktemp -d) | path join database) } }
} else {
	$common
}
let instance = instance --cloud --primary-region primary --regions [{ name: primary }, { name: secondary }] --config $common
let primary = server spawn --instance $instance --region primary --directory (mktemp -d) --preserve-keys --name primary --now '2026-01-01T00:00:00Z' --url (instance region url $instance primary)
let secondary = server spawn --instance $instance --region secondary --directory (mktemp -d) --preserve-keys --name secondary --now '2026-01-01T00:00:00Z' --url (instance region url $instance secondary)

# Populate only the secondary region with the child process and its output.
let path = artifact {
	tangram.ts: '
		export function child() { return tg.file("cached peer child output"); }
		export function sentinel() { return "unreferenced process"; }
		export default async function () {
			await tg.build(child).cached(true);
			return tg.file("tagged parent output");
		}
	'
}
let child = tg --url $secondary.url build --no-tokens --detach $'($path)#child' | referent node
tg --url $secondary.url wait --source=index $child | ignore
tg --url $secondary.url index
let process_sentinel = tg --url $secondary.url build --no-tokens --detach $'($path)#sentinel' | referent node
tg --url $secondary.url wait --source=index $process_sentinel | ignore
tg --url $secondary.url index
let child_data = tg --url $secondary.url process get --source=index --location='local(secondary)' $child | from json
let file = $child_data.output.value | referent node
let blob = tg --url $secondary.url children --local $file | from json | get 0

# Age the cached process so that reusing it must extend its lifetime.
advance_time $secondary 23hr

# The parent must reuse that exact process from the peer cache rather than execute a new child.
let watch = tg --url $secondary.url checkpoint watch process.spawn.child.add --params ({ cached: true, child: $child } | to json --raw) | from json | get watch
let parent = tg --url $primary.url build --no-tokens --detach $path | referent node
let hit = timeout 30s tg --url $secondary.url checkpoint wait process.spawn.child.add $watch 0 | from json
assert equal $hit.params.parent $parent "the cache-serving region should return the child for this parent"
tg --url $secondary.url checkpoint continue process.spawn.child.add $watch 0
tg --url $secondary.url checkpoint unwatch process.spawn.child.add $watch
let outcome = tg --url $primary.url wait --source=index $parent | from json
assert equal $outcome.exit 0 "the parent should finish using its peer cache hit"
tg --url $primary.url index
let children = tg --url $primary.url process children --source=index --local $parent | from json
assert equal ($children | length) 1
assert equal ($children.0.process | referent node) $child "the parent must reuse the process originally built in the secondary region"
assert equal $children.0.cached true "the child must be a cache hit"
let location = $'http://localhost/($children.0.process)' | url parse | get params | where key == location | get value | first
assert equal $location 'local(secondary)' "the indexed child should preserve its region"
let tokens = $children.0.process | referent tokens local
assert ($tokens | is-not-empty) "the indexed child should preserve its authorization token"
let body = $tokens.0 | token body
assert equal $body.resource $child
assert equal $body.permissions [process_parent]
failure (tg --url $secondary.url process get --source=index --location='local(secondary)' $parent | complete) "the parent record should remain in the primary region"
for id in [$file $blob] {
	failure (tg --url $primary.url object get --bytes --location='local(primary)' $id | complete) "using the cached child should not copy its output graph into the parent region"
	success (tg --url $secondary.url object get --bytes --location='local(secondary)' $id | complete)
}

# Propagate the parent tag to both regional indexes before allowing TTL cleaning.
tg --url $primary.url tag put retained-parent $parent
tg --url $primary.url index
tg --url $secondary.url index
assert equal (tg --url $secondary.url tag get retained-parent | from json | get target.id) $parent
advance_time $secondary 2hr
wait_until {
	(tg --url $secondary.url process get --source=index --location='local(secondary)' $process_sentinel | complete).exit_code != 0
} "process GC should collect the unreferenced process while retaining the cached child"

# Touching the cache hit retains the child beyond its original TTL.
success (tg --url $primary.url process get --source=index --local $parent | complete)
success (tg --url $secondary.url process get --source=index --location='local(secondary)' $child | complete) "the cache hit should extend the child TTL"
for id in [$file $blob] {
	success (tg --url $secondary.url object get --bytes --location='local(secondary)' $id | complete) "the touched child should retain its output graph"
}

# The peer copy should expire independently while the primary parent remains tagged.
advance_time $secondary 25hr
wait_until {
	(tg --url $secondary.url process get --source=index --location='local(secondary)' $child | complete).exit_code != 0
} "the peer cached child should expire without an orphan parent reference"
for id in [$file $blob] {
	wait_until {
		(tg --url $secondary.url object get --bytes --location='local(secondary)' $id | complete).exit_code != 0
	} "the cached child's output should expire with the peer child"
}
success (tg --url $primary.url process get --source=index --local $parent | complete) "the parent should remain retained by its tag"
