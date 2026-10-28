use ../lib/test.nu *
use ../lib/nats.nu *

skip_test --cloud

# A messenger outage clears cached authentication and does not make a committed deletion fail.
let nats = spawn_nats
let root = random chars
let common = {
	advanced: { checkpoints: true },
	authentication: { cache: { ttl: 60 }, root: { token: $root }, users: { providers: { insecure: true } } },
	database: { index_queue: { wakeup_interval: 0.01 } },
	messenger: { kind: nats, url: $nats.url },
}
let instance = instance --cloud --primary-region a --regions [{ name: a }, { name: b }] --config $common
let a = server spawn --now '2026-01-01T00:00:00Z' --instance $instance --region a --name a --url (instance region url $instance a)
let b = server spawn --now '2026-01-01T00:00:00Z' --instance $instance --region b --name b --url (instance region url $instance b)
let runner = tg --url $a.url --token $root runner create | from json
let token = tg --url $a.url --token $root runner token create $runner.data.id | from json
success (tg --url $b.url --token $token.token remote list | complete)
let unready = checkpoint watch authentication.cache.unready --params { event: disconnected } --token $root --url $b.url
let resubscribes = [$a $b] | each { |server| checkpoint watch clock.sleep --params { duration: '1s' } --token $root --url $server.url }
stop_nats $nats
checkpoint pass $unready | ignore
checkpoint unwatch $unready

# Advance each server's clock past its resubscribe delay once the outage starts it.
$resubscribes | each { |sleep| checkpoint pass $sleep; checkpoint unwatch $sleep } | ignore
advance_times [$a $b] 1sec
success (tg --url $a.url --token $root runner token delete $runner.data.id $token.id | complete) 'publication failure must not report a committed deletion as failed'
failure (tg --url $b.url --token $token.token remote list | complete) 'the disconnected server retained cached authentication'

let readies = [$a $b] | each { |server| checkpoint watch authentication.cache.ready --token $root --url $server.url }
let restarted = spawn_nats --listen-port $nats.port
$readies | each { |ready| checkpoint pass $ready; checkpoint unwatch $ready } | ignore
let token = tg --url $a.url --token $root runner token create $runner.data.id | from json
success (tg --url $b.url --token $token.token remote list | complete)
let invalidated = checkpoint watch authentication.cache.invalidated --params { kind: token } --token $root --url $b.url
tg --url $a.url --token $root runner token delete $runner.data.id $token.id
checkpoint pass $invalidated | ignore
checkpoint unwatch $invalidated
failure (tg --url $b.url --token $token.token remote list | complete) 'the recovered subscription did not invalidate the token'
stop_nats $restarted
