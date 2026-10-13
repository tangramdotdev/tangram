use ../lib/test.nu *
use ../lib/nats.nu *
use ../lib/stripe.nu *

# All named-node replacement paths revoke cached user tokens after committing.
let nats = spawn_nats
let stripe = spawn_stripe
let root = random chars
let secret = 'whsec_mock'
let common = {
	authentication: { cache: { ttl: 60 }, root: { token: $root }, users: { providers: { insecure: true } } },
	billing: { stripe: { secret_key: 'sk_test_mock', url: $stripe.url, webhook_secret: $secret } },
	database: { kind: sqlite, index_queue: { wakeup_interval: 0.01 }, path: (mktemp -d | path join database.sqlite3) },
	messenger: { kind: nats, url: $nats.url },
}
let instance = instance --primary-region a --regions [{ name: a }, { name: b }] --config $common
let a = server spawn --instance $instance --region a --name a --directory (mktemp -d) --url (instance region url $instance a)
let b = server spawn --instance $instance --region b --name b --directory (mktemp -d) --url (instance region url $instance b)
let source = server spawn --name source --config { remotes: { default: { token: $root, url: $a.url } } }
let target = tg --url $a.url --token $root put --no-tokens 'tg.file("replacement")' | referent node

for mode in [single batch sync] {
	let user = tg --url $a.url login --verbose --email $'($mode)@example.com' --name $mode | from json
	with-env { BROWSER: 'false' } { tg --url $a.url --token $user.token user billing manage }
	let ready = { data: { object: { id: cus_mock } }, id: $'evt_ready_($mode)', type: customer.updated }
	assert equal (send_stripe_webhook $a $secret $ready) 200
	success (tg --url $a.url --token $user.token remote list | complete)
	success (tg --url $b.url --token $user.token remote list | complete)
	match $mode {
		single => { tg --url $a.url --token $root tag put --force $mode $target },
		batch => {
			let socket = $a.url | str replace 'http+unix://' '' | url decode
			{ force: true, tags: [{ specifier: $mode, target: { kind: object, id: $target } }] }
			| http post --content-type application/json --headers { Authorization: $'Bearer ($root)' } --unix-socket $socket 'http://localhost/tags/batch'
			| ignore
		},
		sync => {
			let target = tg --url $source.url put --no-tokens 'tg.file("replacement")' | referent node
			tg --url $source.url tag put $mode $target
			tg --url $source.url push --force $mode
		},
	}
	for server in [$a $b] {
		wait_until { (tg --url $server.url --token $user.token remote list | complete).exit_code != 0 } $'($mode) replacement retained the deleted user token' --timeout 5sec
	}
}
stop_stripe $stripe
stop_nats $nats
