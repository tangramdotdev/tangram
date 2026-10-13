use ../lib/test.nu *
use ../lib/nats.nu *

# A rejected invalidation subscription keeps authentication on the database path.
let nats = spawn_nats --config {
	authorization: {
		users: [{
			user: test,
			password: test,
			permissions: { publish: '>', subscribe: { deny: [authentication.invalidate] } },
		}],
	},
}
let root = random chars
let common = {
	authentication: { cache: { ttl: 60 }, root: { token: $root }, users: { providers: { insecure: true } } },
	database: { kind: sqlite, index_queue: { wakeup_interval: 0.01 }, path: (mktemp -d | path join database.sqlite3) },
	messenger: { kind: nats, password: test, url: $nats.url, username: test },
}
let instance = instance --primary-region a --regions [{ name: a }, { name: b }] --config $common
let a = server spawn --instance $instance --region a --name a --directory (mktemp -d) --url (instance region url $instance a)
let b = server spawn --instance $instance --region b --name b --directory (mktemp -d) --url (instance region url $instance b)
wait_until { open --raw $b.log | str contains 'disabling authentication caching' } 'the subscription rejection was not handled'
let runner = tg --url $a.url --token $root runner create | from json
let token = tg --url $a.url --token $root runner token create $runner.data.id | from json
success (tg --url $b.url --token $token.token remote list | complete)
tg --url $a.url --token $root runner token delete $runner.data.id $token.id
failure (tg --url $b.url --token $token.token remote list | complete) 'the server cached authentication despite its rejected subscription'
# Correcting permissions on the live connection restores subscription delivery and caching.
let connections = http get $'($nats.monitor_url)/connz' | get connections | get cid | sort
reload_nats $nats { authorization: { users: [{ user: test, password: test, permissions: { publish: '>', subscribe: '>' } }] } }
wait_until {
	try {
		(http get $'($nats.monitor_url)/subsz?subs=1').subscriptions_list
		| where subject == 'authentication.invalidate'
		| length
		| $in == 2
	} catch { false }
} 'the corrected subscriptions did not recover'
assert equal (http get $'($nats.monitor_url)/connz' | get connections | get cid | sort) $connections 'permission recovery should not require reconnecting'
let token = tg --url $a.url --token $root runner token create $runner.data.id | from json
success (tg --url $b.url --token $token.token remote list | complete)
tg --url $a.url --token $root runner token delete $runner.data.id $token.id
wait_until { (tg --url $b.url --token $token.token remote list | complete).exit_code != 0 } 'the restored subscription did not invalidate the token' --timeout 5sec
stop_nats $nats
