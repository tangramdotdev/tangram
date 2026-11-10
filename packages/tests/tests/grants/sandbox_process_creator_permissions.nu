use ../../util/test.nu *

# Both the process creator and the process owner receive parent authority.
let local = server spawn --name local --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let carol = tg login --verbose --name carol | from json
let sandbox = tg --token $alice.token sandbox create --no-tokens | referent node
tg --token $alice.token grant $bob.user.id parent $sandbox
let path = artifact { tangram.ts: 'export default () => tg.file("creator-access");' }
let process = tg --token $bob.token spawn --no-tokens $'--sandbox=($sandbox)' $path | referent node
tg --token $bob.token wait $process | ignore
tg --token $bob.token index
success (tg --token $bob.token process get $process | complete)
tg --token $alice.token index
success (tg --token $alice.token process get $process | complete)
tg --token $bob.token index
let data = tg --token $bob.token process get $process | from json
assert equal $data.creator $bob.user.id
assert equal $data.owner $alice.user.id

# Both keep access after the sandbox is destroyed, and a third user has none.
tg --token $alice.token sandbox destroy $sandbox
tg --token $alice.token sandbox wait $sandbox | ignore
tg --token $alice.token index
success (tg --token $alice.token process get $process | complete)
tg --token $bob.token index
success (tg --token $bob.token process get $process | complete)
tg --token $carol.token index
failure (tg --token $carol.token process get $process | complete)
