use ../lib/test.nu *

# A tag is billed to an organization that is created in the same sync transaction.

let remote_destination = server spawn --cloud --name remote-destination --config { usage: true }
let local_source = server spawn --name local-source --config {
	remotes: { default: { url: $remote_destination.url } },
	usage: true,
}
let organization = tg --url $local_source.url organization create acme | from json
let object = tg --url $local_source.url put --no-tokens 'tg.file("hello")' | referent node
tg --url $local_source.url tag put -p acme/owned $object

# Sync the organization and its tag before either exists in the destination index.
tg --url $local_source.url push --ancestors=always acme/owned
tg --url $remote_destination.url index
let usage = tg --url $remote_destination.url organization usage $organization.id | from json
assert ($usage.object_count >= 1) "an organization tag must charge the organization"
