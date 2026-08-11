use ../../test.nu *

# Building with a nested tag should pull the tag's missing ancestors from a remote, the way tagging
# does. Every spelling of the flag is covered: the aliases, the explicit policies, and the last flag
# winning when both are given.

let remote = spawn --name remote
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let path = artifact {
	tangram.ts: 'export default function () { return "hello"; }'
}

# Create a chain of ancestors on the remote only.
def create_remote_ancestors [remote: record, root: string] {
	tg --url $remote.url group create $root | ignore
	tg --url $remote.url group create $"($root)/builds" | ignore
	tg --url $remote.url group create $"($root)/builds/1.0.0" | from json
}

# Every spelling that never pulls should refuse, since the ancestors stay on the remote.
create_remote_ancestors $remote refused
let refusals = [
	[--no-pull-tag-ancestors]
	[--no-pull-tag-parents]
	['--pull-tag-ancestors=never']
	['--no-pull-tag-ancestors=true']
	[--pull-tag-ancestors --no-pull-tag-ancestors]
]
for flags in $refusals {
	let refused = tg --url $local.url build ...$flags --tag refused/builds/1.0.0/default $path | complete
	failure $refused $"building with ($flags | str join ' ') should fail without pulling the ancestors"
	assert ($refused.stderr | str contains "the parent does not exist")
}
failure (tg --url $local.url group get --local refused | complete) "a refused build should not pull any ancestors"

# Every spelling that pulls should take the ancestors from the remote.
let pulls = [
	{ root: default, flags: [] }
	{ root: bare, flags: [--pull-tag-ancestors] }
	{ root: always, flags: ['--pull-tag-ancestors=always'] }
	{ root: missing, flags: ['--pull-tag-ancestors=missing'] }
	{ root: alias, flags: ['--pull-tag-parents=missing'] }
	{ root: negated, flags: ['--no-pull-tag-ancestors=false'] }
	{ root: override, flags: [--no-pull-tag-ancestors --pull-tag-ancestors] }
]
for pull in $pulls {
	let version = create_remote_ancestors $remote $pull.root
	let specifier = $"($pull.root)/builds/1.0.0/default"
	let output = tg --url $local.url build ...$pull.flags --tag $specifier $path | complete
	success $output $"building with ($pull.flags | str join ' ') should pull the tag's ancestors"

	let tag = tg --url $local.url tag get $specifier | from json
	assert equal $tag.specifier $specifier "the tag should keep its specifier"
	assert equal $tag.parent $version.id "the tag should hold the ancestor from the remote"
	let pulled = tg --url $local.url group get --local $"($pull.root)/builds/1.0.0" | from json
	assert equal $pulled.id $version.id "the ancestors should come from the remote"
}

# Missing should keep a conflicting local root, leaving the tag's parent missing.
let local_root = tg --url $local.url group create conflict | from json
let version = create_remote_ancestors $remote conflict
assert not equal $local_root.id (tg --url $remote.url group get conflict | from json | get id)
let refused = tg --url $local.url build --tag conflict/builds/1.0.0/default $path | complete
failure $refused "building should fail while a conflicting local root shadows the remote ancestors"
assert equal (tg --url $local.url group get --local conflict | from json | get id) $local_root.id

# Always should replace the conflicting local root with the remote's.
let output = tg --url $local.url build '--pull-tag-ancestors=always' --tag conflict/builds/1.0.0/default $path | complete
success $output "building should replace a conflicting local root when always pulling"
assert equal (tg --url $local.url group get --local conflict/builds/1.0.0 | from json | get id) $version.id

# Creating should not consult the remote when pulling is disabled.
let version = create_remote_ancestors $remote created
let output = tg --url $local.url build --create-tag-ancestors --no-pull-tag-ancestors --tag created/builds/1.0.0/default $path | complete
success $output "building should create the tag's ancestors locally without pulling"
let created = tg --url $local.url group get --local created/builds/1.0.0 | from json
assert not equal $created.id $version.id "the created ancestors should not come from the remote"
