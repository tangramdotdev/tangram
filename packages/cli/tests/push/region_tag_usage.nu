use ../lib/test.nu *

# A tag charges storage only in regions where its target is present, including
# when the target arrives after the tag.

def target_permissions [url: string, token: string, tag: string] {
	let target = tg --url $url --token $token children --verbose $tag | from json | get data.0
	$target.options?.tokens?.local? | default [] | each {|token|
		$token | split row '.' | get 1 | decode base64 | decode utf-8 | from json | get permissions
	} | flatten
}

let region_a_directory = mktemp -d
let region_b_directory = mktemp -d
let database_path = mktemp -d | path join database
let regions = [
	{ name: a },
	{ name: b },
]
let common = {
	authentication: { users: { providers: { insecure: true } } },
	database: {
		kind: sqlite,
		index_queue: { wakeup_interval: 0.01 },
		path: $database_path,
	},
	usage: true,
}
let instance = instance --primary-region a --regions $regions --config $common
let remote_region_a = server spawn --instance $instance --region a --preserve-keys --name remote-region-a --directory $region_a_directory --url (instance region url $instance a)
let remote_region_b = server spawn --instance $instance --region b --preserve-keys --name remote-region-b --directory $region_b_directory --url (instance region url $instance b)
let alice = tg --url $remote_region_a.url login --verbose --name alice | from json
let bob = tg --url $remote_region_a.url login --verbose --name bob | from json
let local = server spawn --name local --config {
	remotes: {
		a: { token: $bob.token, url: $remote_region_a.url }
		b: { token: $alice.token, url: $remote_region_b.url }
	}
}

# Alice pushes a tagged object to region B. The tag is global, but the object
# and its storage usage are regional.
let object = tg --url $local.url put --no-tokens 'tg.file("hello")' | referent node
tg --url $local.url tag put owned $object
tg --url $local.url push --remote=b owned

wait_until {
	(tg --url $remote_region_b.url --token $alice.token user usage --location='local(b)' | from json | get object_count) >= 1
} "Alice's tag should charge storage in region B"
let tag = tg --url $remote_region_b.url --token $alice.token tag get owned | from json
assert (target_permissions $remote_region_b.url $alice.token $tag.id | any { |permission| $permission in ['object_node' 'object_subtree'] }) "the tag should retain Alice's target permissions"
let usage = tg --url $remote_region_a.url --token $alice.token user usage --location='local(a)' | from json
assert equal $usage.object_count 0 "the tag must not charge storage for an absent object"

# Bob pushes only the object to region A. Alice's tag retained the target
# permissions recorded in region B, so it grants her access independently of
# Bob's normal push accounting and must charge her usage.
tg --url $local.url push --remote=a $object
success (tg --url $remote_region_a.url --token $alice.token object get --bytes --local $object | complete) "Alice's tag should grant access to the object in region A"
wait_until {
	(tg --url $remote_region_a.url --token $alice.token user usage --location='local(a)' | from json | get object_count) >= 1
} "the existing tag should charge Alice when its target arrives in region A"

# A tag with no target permissions must not charge its owner in either region.
let region_a_usage = tg --url $remote_region_a.url --token $alice.token user usage --location='local(a)' | from json
let region_b_usage = tg --url $remote_region_b.url --token $alice.token user usage --location='local(b)' | from json
let bob_local = server spawn --name bob-local --config {
	remotes: {
		a: { token: $bob.token, url: $remote_region_a.url }
		b: { token: $bob.token, url: $remote_region_b.url }
	}
}
let private_object = tg --url $bob_local.url put --no-tokens 'tg.file("private")' | referent node
tg --url $bob_local.url push --remote=b $private_object
failure (tg --url $remote_region_b.url --token $alice.token object get --bytes --local $private_object | complete) "Alice must not access Bob's private object"
tg --url $remote_region_b.url --token $alice.token tag put alice/no-access $private_object
tg --url $remote_region_b.url index
let tag = tg --url $remote_region_b.url --token $alice.token tag get alice/no-access | from json
assert (target_permissions $remote_region_b.url $alice.token $tag.id | is-empty) "the tag must not record unavailable target permissions"
let usage = tg --url $remote_region_b.url --token $alice.token user usage --location='local(b)' | from json
assert equal $usage.object_count $region_b_usage.object_count "an inaccessible tag target must not charge Alice in region B"

tg --url $bob_local.url push --remote=a $private_object
tg --url $remote_region_a.url index
failure (tg --url $remote_region_a.url --token $alice.token object get --bytes --local $private_object | complete) "the tag must not grant Alice access in region A"
let usage = tg --url $remote_region_a.url --token $alice.token user usage --location='local(a)' | from json
assert equal $usage.object_count $region_a_usage.object_count "an inaccessible tag target must not charge Alice in region A"
