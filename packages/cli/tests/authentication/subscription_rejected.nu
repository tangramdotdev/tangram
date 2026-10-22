use ../lib/test.nu *
use ../lib/nats.nu *

skip_if_no_cloud

# A rejected invalidation subscription keeps authentication on the database path.
let nats = spawn_nats --config {
	authorization: {
		users: [{
			user: test,
			password: test,
			permissions: { publish: '>', subscribe: { deny: ['*.authentication.invalidate'] } },
		}],
	},
}
let root = random chars
let common = {
	advanced: { checkpoints: true },
	authentication: { cache: { ttl: 60 }, root: { token: $root }, users: { providers: { insecure: true } } },
	database: { index_queue: { wakeup_interval: 0.01 } },
	messenger: { kind: nats, password: test, url: $nats.url, username: test },
}
let instance = instance --cloud --primary-region a --regions [{ name: a }, { name: b }] --config $common
let a = server spawn --instance $instance --region a --name a --url (instance region url $instance a)
let b = server spawn --instance $instance --region b --name b --url (instance region url $instance b)
wait_for_log $b 'disabling authentication caching'
let runner = tg --url $a.url --token $root runner create | from json
let token = tg --url $a.url --token $root runner token create $runner.data.id | from json
success (tg --url $b.url --token $token.token remote list | complete)
tg --url $a.url --token $root runner token delete $runner.data.id $token.id
failure (tg --url $b.url --token $token.token remote list | complete) 'the server cached authentication despite its rejected subscription'
# Correcting permissions restores subscription delivery and caching.
reload_nats $nats { authorization: { users: [{ user: test, password: test, permissions: { publish: '>', subscribe: '>' } }] } }
wait_until {
	try {
		(http get $'($nats.monitor_url)/subsz?subs=1').subscriptions_list
		| where subject == $'($instance.id).authentication.invalidate'
		| length
		| $in == 2
	} catch { false }
} 'the corrected subscriptions did not recover'
let token = tg --url $a.url --token $root runner token create $runner.data.id | from json
success (tg --url $b.url --token $token.token remote list | complete)
let invalidated = checkpoint watch authentication.cache.invalidated --params { kind: token } --token $root --url $b.url
tg --url $a.url --token $root runner token delete $runner.data.id $token.id
checkpoint pass $invalidated | ignore
checkpoint unwatch $invalidated
failure (tg --url $b.url --token $token.token remote list | complete) 'the restored subscription did not invalidate the token'
stop_nats $nats
