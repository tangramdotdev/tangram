use ../../test.nu *

# A tag is billed to an organization that is created in the same sync transaction.

let destination = server spawn --cloud --name destination --config { usage: true }
let source = server spawn --name source --config {
	remotes: { default: { url: $destination.url } },
	usage: true,
}
let organization = tg --url $source.url organization create acme | from json
let object = tg --url $source.url put 'tg.file("hello")' | str trim
tg --url $source.url tag put -p acme/owned $object

# Sync the organization and its tag before either exists in the destination index.
tg --url $source.url push --ancestors=always acme/owned
tg --url $destination.url index
let usage = tg --url $destination.url organization usage $organization.id | from json
assert ($usage.object_count >= 1) "an organization tag must charge the organization"
