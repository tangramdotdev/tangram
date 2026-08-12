use ../../test.nu *

# Users retain their IDs and emails, and organizations retain their IDs, when synced.

let source = spawn --cloud --name source --config { authentication: { users: { providers: { insecure: true } } } }
let source_user = tg --url $source.url login --verbose --name source-user --email source-user@example.com | from json
let source_organization = tg --url $source.url --token $source_user.token organization create source-organization | from json
let destination = spawn --name destination --config {
	remotes: { default: { url: $source.url, token: $source_user.token } },
}

tg --url $destination.url pull $source_user.user.id $source_organization.id
tg --url $destination.url index

let destination_user = tg --url $destination.url user get source-user | from json
let destination_organization = tg --url $destination.url organization get source-organization | from json
assert equal $destination_user.id $source_user.user.id
assert equal $destination_user.emails $source_user.user.emails
assert equal $destination_organization.id $source_organization.id
