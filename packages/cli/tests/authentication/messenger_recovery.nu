use ../lib/test.nu *
use ../lib/nats.nu *

skip_if_no_cloud

# A messenger outage clears cached authentication and does not make a committed deletion fail.
let nats = spawn_nats
let root = random chars
let common = {
	authentication: { cache: { ttl: 60 }, root: { token: $root }, users: { providers: { insecure: true } } },
	database: { index_queue: { wakeup_interval: 0.01 } },
	messenger: { kind: nats, url: $nats.url },
}
let instance = instance --cloud --primary-region a --regions [{ name: a }, { name: b }] --config $common
let a = server spawn --instance $instance --region a --name a --url (instance region url $instance a)
let b = server spawn --instance $instance --region b --name b --url (instance region url $instance b)
let runner = tg --url $a.url --token $root runner create | from json
let token = tg --url $a.url --token $root runner token create $runner.data.id | from json
success (tg --url $b.url --token $token.token remote list | complete)
stop_nats $nats
wait_until { try { http get $'($nats.monitor_url)/healthz' | ignore; false } catch { true } } 'NATS did not stop'
success (tg --url $a.url --token $root runner token delete $runner.data.id $token.id | complete) 'publication failure must not report a committed deletion as failed'
wait_until { (tg --url $b.url --token $token.token remote list | complete).exit_code != 0 } 'the disconnected server retained cached authentication' --timeout 5sec

let restarted = spawn_nats --listen-port $nats.port
wait_until {
	try {
		let subscriptions = http get $'($restarted.monitor_url)/subsz?subs=1'
		($subscriptions.subscriptions_list | where subject == $'($instance.id).authentication.invalidate' | length) == 2
	} catch { false }
} 'authentication subscriptions did not recover'
let token = tg --url $a.url --token $root runner token create $runner.data.id | from json
success (tg --url $b.url --token $token.token remote list | complete)
tg --url $a.url --token $root runner token delete $runner.data.id $token.id
wait_until { (tg --url $b.url --token $token.token remote list | complete).exit_code != 0 } 'the recovered subscription did not invalidate the token' --timeout 5sec
stop_nats $restarted
