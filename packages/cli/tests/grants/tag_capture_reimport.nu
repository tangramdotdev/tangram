use ../lib/test.nu *

# Reimporting a deleted tag with the same id must never reuse an earlier capture version.

let root_token = random chars
let remote = server spawn --name remote --config {
	authentication: { root: { token: $root_token } }
}
let original = tg --url $remote.url --token $root_token put 'tg.file("original")' | str trim
let replacement = tg --url $remote.url --token $root_token put 'tg.file("replacement")' | str trim
tg --url $remote.url --token $root_token tag put retained $original --public
tg --url $remote.url --token $root_token index
let source = tg --url $remote.url --token $root_token tag get retained | from json
let local = server spawn --name local --config {
	database: { kind: sqlite, path: "database.sqlite3" }
	authentication: { root: { token: $root_token } }
	remotes: { default: { url: $remote.url, token: $root_token, trusted: true } }
}
tg --url $local.url --token $root_token pull retained
tg --url $local.url --token $root_token index
let first = tg --url $local.url --token $root_token tag get retained | from json
assert equal $first.id $source.id
let database = $local.directory | path join database.sqlite3
let first_version = open $database | query db "select version from tags" | get version.0

# Delete a locally retargeted version, rather than the version originally imported.
tg --url $local.url --token $root_token pull $replacement
tg --url $local.url --token $root_token tag put --force retained $replacement
tg --url $local.url --token $root_token index
let retargeted_version = open $database | query db "select version from tags" | get version.0
assert ($retargeted_version != $first_version)
tg --url $local.url --token $root_token tag delete retained
tg --url $local.url --token $root_token pull retained
tg --url $local.url --token $root_token index
let second = tg --url $local.url --token $root_token tag get retained | from json
assert equal $second.id $first.id
assert equal $second.target.id $original
let second_version = open $database | query db "select version from tags" | get version.0
assert ($second_version != $first_version)
assert ($second_version != $retargeted_version) "reimport must invalidate captures for every version before deletion."

tg --url $local.url --token $root_token tag delete retained
tg --url $local.url --token $root_token pull retained
tg --url $local.url --token $root_token index
let third = tg --url $local.url --token $root_token tag get retained | from json
assert equal $third.id $first.id
let third_version = open $database | query db "select version from tags" | get version.0
assert ($third_version != $second_version) "each reimport after deletion must have a fresh version."
