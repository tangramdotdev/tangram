use ../lib/test.nu *

# Tag creation touches a regional target before its tag reference is indexed.

let database_path = (mktemp -d) | path join database
let regions = [{ name: primary }, { name: secondary }]
let instance = instance --primary-region primary --regions $regions --config {
	advanced: { checkpoints: true, single_directory: false, single_process: false },
	checkouts: false,
	database: { kind: sqlite, path: $database_path, index_queue: { wakeup_interval: 0.01 } },
	indexer: { cleaning: { poll_interval: 0.01 } },
	object: { ttl: 86400, ttt: 3600 },
}
let primary = server spawn --instance $instance --region primary --name primary --directory (mktemp -d) --now '2026-01-01T00:00:00Z' --url (instance region url $instance primary)
let secondary = server spawn --instance $instance --region secondary --name secondary --directory (mktemp -d) --now '2026-01-01T00:00:00Z' --url (instance region url $instance secondary)
let target = tg --url $secondary.url put --no-tokens 'tg.file("retain me")' | referent node
let untouched = tg --url $secondary.url put --no-tokens 'tg.file("collect me")' | referent node
tg --url $secondary.url index

# Pause tag propagation while the object approaches its original expiration.
advance_time $primary 23hr
advance_time $secondary 23hr
let watch = tg --url $secondary.url checkpoint watch indexer.database_index_queue.batch | from json | get watch
tg --url $primary.url tag put retained $target
tg --url $secondary.url checkpoint wait indexer.database_index_queue.batch $watch 0 | ignore
let clean_watch = tg --url $secondary.url checkpoint watch cleaning.object.delete --params ({ object: $untouched } | to json) | from json | get watch
advance_time $secondary 2hr
tg --url $secondary.url checkpoint wait cleaning.object.delete $clean_watch 0 | ignore
let socket = $secondary.url | str replace 'http+unix://' '' | url decode
let response = http get --full --allow-errors --max-time 5sec --headers { Accept: 'application/json' } --unix-socket $socket $'http://localhost/objects/($target)?location=local%28secondary%29'
assert equal $response.status 200 "the touched target should remain readable while tag propagation is paused"
tg --url $secondary.url checkpoint continue cleaning.object.delete $clean_watch 0
tg --url $secondary.url checkpoint unwatch cleaning.object.delete $clean_watch

# Once propagated, the tag retains the object beyond the refreshed TTL.
tg --url $secondary.url checkpoint continue indexer.database_index_queue.batch $watch 0
tg --url $secondary.url checkpoint unwatch indexer.database_index_queue.batch $watch
tg --url $secondary.url index
advance_time $secondary 25hr
let expired = tg --url $secondary.url put --no-tokens 'tg.file("another untagged object")' | referent node
tg --url $secondary.url index
advance_time $secondary 25hr
wait_until {
	(tg --url $secondary.url object get --location='local(secondary)' $expired | complete).exit_code != 0
} "cleaning should run after the refreshed TTL expires"
success (tg --url $secondary.url object get --bytes --location='local(secondary)' $target | complete)

# Removing the tag makes the target collectible again.
tg --url $primary.url tag delete retained
tg --url $secondary.url index
wait_until {
	(tg --url $secondary.url object get --location='local(secondary)' $target | complete).exit_code != 0
} "the target should expire after its tag is removed"

# Both write APIs reject a target that has already been collected.
let socket = $primary.url | str replace 'http+unix://' '' | url decode
let body = { specifier: missing, target: { kind: object, id: $target } } | to json --raw
let response = http put --full --allow-errors --max-time 10sec --content-type application/json --unix-socket $socket 'http://localhost/tags' $body
assert ($response.status != 200) "tag creation should fail for a collected target"
assert ($response.body | to text | str contains 'failed to touch the tag target') "tag creation should report the failed touch"
let body = { tags: [{ specifier: missing-batch, target: { kind: object, id: $target } }] } | to json --raw
let response = http post --full --allow-errors --max-time 10sec --content-type application/json --unix-socket $socket 'http://localhost/tags/batch' $body
assert ($response.status != 200) "a tag batch should fail for a collected target"
assert ($response.body | to text | str contains 'failed to touch the tag target') "a tag batch should report the failed touch"
let tags = open $database_path | query db "select name from tags where name in ('missing', 'missing-batch')"
assert ($tags | is-empty) "a failed touch should not create a tag"
