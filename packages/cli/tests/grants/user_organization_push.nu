use ../lib/test.nu *

# Users retain their IDs and emails, and organizations retain their IDs, when synced.

let local_source = server spawn --cloud --name local-source --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg --url $local_source.url login --verbose --name alice --email source-user@example.com | from json
let source_organization = tg --url $local_source.url --token $alice.token organization create source-organization | from json
let remote_destination = server spawn --name remote-destination --config {
	remotes: { default: { url: $local_source.url, token: $alice.token } },
}

tg --url $remote_destination.url pull $alice.user.id $source_organization.id
tg --url $remote_destination.url index

let destination_user = tg --url $remote_destination.url user get alice | from json
let destination_organization = tg --url $remote_destination.url organization get source-organization | from json
assert equal $destination_user.id $alice.user.id
assert equal $destination_user.emails $alice.user.emails
assert equal $destination_organization.id $source_organization.id
