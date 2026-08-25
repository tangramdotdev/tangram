use ../../test.nu *

# Building with a nested tag should be able to create the tag's missing ancestor groups, the way
# tagging and creating a group do, and should keep refusing by default. Every spelling of the flag is
# covered: the aliases, the explicit boolean values, and the last flag winning when both are given.

let server = server spawn

let path = artifact {
	tangram.ts: 'export default function () { return "hello"; }'
}

# Every spelling that leaves creating disabled should refuse.
let refusals = [
	[]
	[--no-create-tag-ancestors]
	[--no-create-tag-parents]
	['--create-tag-ancestors=false']
	['--no-create-tag-ancestors=true']
	[--create-tag-ancestors --no-create-tag-ancestors]
]
for flags in $refusals {
	let refused = tg build ...$flags --tag refused/builds/1.0.0/default $path | complete
	failure $refused $"building with ($flags | str join ' ') should fail when the ancestors do not exist"
	assert ($refused.stderr | str contains "the parent does not exist")
}
failure (tg group get refused | complete) "a refused build should not create any ancestors"

# Every spelling that enables creating should create the whole chain.
let creations = [
	{ root: ancestors, flags: [--create-tag-ancestors] }
	{ root: parents, flags: [--create-tag-parents] }
	{ root: explicit, flags: ['--create-tag-ancestors=true'] }
	{ root: negated, flags: ['--no-create-tag-ancestors=false'] }
	{ root: override, flags: [--no-create-tag-ancestors --create-tag-ancestors] }
]
for creation in $creations {
	let specifier = $"($creation.root)/builds/1.0.0/default"
	let output = tg build ...$creation.flags --tag $specifier $path | complete
	success $output $"building with ($creation.flags | str join ' ') should create the tag's ancestors"

	let tag = tg tag get $specifier | from json
	assert equal $tag.specifier $specifier "the tag should keep its specifier"
	assert equal $tag.target.kind "process" "the tag should point to the process"

	let root = tg group get $creation.root | from json
	let builds = tg group get $"($creation.root)/builds" | from json
	let version = tg group get $"($creation.root)/builds/1.0.0" | from json
	assert equal $root.specifier $creation.root "the root ancestor should exist"
	assert equal $builds.parent $root.id "each ancestor should hold the one above it"
	assert equal $version.parent $builds.id "the whole chain should exist"
	assert equal $tag.parent $version.id "the tag should hold the last ancestor"
}
