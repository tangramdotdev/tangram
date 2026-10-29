use ../../util/test.nu *

# Account and usage commands select the default remote when their local features are disabled.
let remote = server spawn --name remote --config {
	accounts: true,
	authentication: { users: { providers: { insecure: true } } },
	runners: true,
	usage: true,
}
let alice = tg login --verbose --name alice | from json
let expected_usage = tg usage | from json
let local = server spawn --name local --config {
	accounts: false,
	runners: false,
	remotes: { default: { url: $remote.url } },
}

let login = tg login --verbose --name alice | from json
assert equal $login.user.id $alice.user.id
assert equal $login.location 'remote'
assert equal (tg whoami | from json).id $alice.user.id
assert equal (tg user get alice | from json).id $alice.user.id
assert equal (tg usage | from json) $expected_usage
assert equal (tg user usage | from json) $expected_usage
assert equal (tg usage alice | from json) $expected_usage

let token = tg user token create | from json
assert ((tg user token list --all | from json | get id) | any { $in == $token.id })
tg user token delete $token.id
assert not ((tg user token list --all | from json | get id) | any { $in == $token.id })

let group = tg group create foo | from json
let organization = tg organization create acme | from json
assert equal (tg group get foo | from json).id $group.id
assert equal (tg organization get acme | from json).id $organization.id
assert equal (tg group members list foo | from json) (tg --url $remote.url --token $alice.token group members list foo | from json)
assert equal (tg organization members list acme | from json) (tg --url $remote.url --token $alice.token organization members list acme | from json)
let member = tg group create member | from json
tg group members add foo $member.id
assert ($member.id in (tg group members list foo | from json))
tg group members remove foo $member.id
tg organization members add acme $member.id
assert ($member.id in (tg organization members list acme | from json))
tg organization members remove acme $member.id
tg group delete member
let usage = tg organization usage acme | from json
assert equal (tg usage acme | from json) $usage

# Local namespace storage and generic reads remain independent of account management.
let artifact = tg put --no-tokens 'tg.file("hello")' | referent node
tg tag put foo/bar $artifact
assert equal (tg group get --local foo | from json).id $group.id
assert equal (tg get foo | from json).id $group.id
assert equal (tg user get --local --remote alice | from json).id $alice.user.id
assert equal (tg group get --local --remote foo | from json).id $group.id
assert equal (tg organization get --local --remote acme | from json).id $organization.id
assert equal (tg tag get --local foo/bar | from json).target.id $artifact
tg push foo/bar
assert equal (tg --url $remote.url tag get --local foo/bar | from json).target.id $artifact

let output = tg group create --local disabled | complete
failure $output
assert ($output.stderr | str contains 'local account management is disabled')
let output = tg login --local --name alice | complete
failure $output
assert ($output.stderr | str contains 'local account management is disabled')
let output = tg usage --local | complete
failure $output
assert ($output.stderr | str contains 'usage tracking is disabled')
let output = tg organization usage --local acme | complete
failure $output
assert ($output.stderr | str contains 'usage tracking is disabled')

tg --url $remote.url --token $alice.token tag delete --local foo/bar
tg group delete foo
tg organization delete acme
tg logout
failure (tg whoami | complete)
let remote_config = tg remote get default | from json
assert ($remote_config.token? == null)
