use ../lib/test.nu *

# A process may use its owner's remote, but it must not receive the owner's remote authentication token.

let remote = server spawn --name remote --config { authentication: { users: { providers: { insecure: true } } } }
let local = server spawn --name local --config { authentication: { users: { providers: { insecure: true } } } }

let remote_alice = tg --url $remote.url login --verbose --name alice | from json
let local_alice = tg --url $local.url login --verbose --name alice | from json
let secret = tg --url $remote.url --token $remote_alice.token write --no-tokens "remote secret" | referent node
tg --url $local.url --token $local_alice.token remote put default $remote.url
let login = tg --url $local.url --token $local_alice.token login --remote=default --verbose --name alice | from json
assert equal $login.user.id $remote_alice.user.id

# Run a command that exposes its process token and stays alive.
let path = artifact {
	tangram.ts: '
		export default async function () {
			console.log(tg.process.env.TANGRAM_TOKEN);
			await tg.sleep(60);
		}
	'
}
let process = tg --url $local.url --token $local_alice.token run --network=true --detach --verbose $path | from json
wait_until { (tg --url $local.url --token $local_alice.token log $process.process | str trim | str length) > 0 } "the process should log its token"
let process_token = tg --url $local.url --token $local_alice.token log $process.process | str trim

# The process can resolve the remote but must not receive its owner's credential.
let output = tg --url $local.url --token $process_token remote get default | from json
assert equal $output.url $remote.url

# The process does not receive credentials that would let it impersonate its owner.
failure (tg --url $remote.url --token $process_token get --bytes $secret | complete) "the local process token must not authorize a private remote object"
let owner_remote = tg --url $local.url --token $local_alice.token remote get default | from json
success (tg --url $remote.url --token $owner_remote.token get --bytes $secret | complete) "the owner retains access to the remote credential"
assert (($output | get --optional token) == null) "a process must not receive its owner's remote authentication token"

# Internal forwarding still uses the stored credential.
success (tg --url $local.url --token $process_token get --remote=default --bytes $secret | complete) "the server can still authenticate remote operations"

tg --url $local.url --token $local_alice.token cancel $process.process $process.lease
tg --url $local.url --token $local_alice.token wait $process.process
