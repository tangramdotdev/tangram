use ../lib/test.nu *

# Node grants permit reading and sharing, while parent grants permit spawn and destroy.
let local = server spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let carol = tg login --verbose --name carol | from json
let sandbox = tg --token $alice.token sandbox create --no-tokens --no-network | referent node
tg --token $alice.token grant $bob.user.id node $sandbox
tg index
success (tg --token $bob.token sandbox get $sandbox | complete)
failure (tg --token $bob.token sandbox destroy $sandbox | complete)
let path = artifact { tangram.ts: 'export default () => tg.file("output");' }
failure (tg --token $bob.token run $'--sandbox=($sandbox)' $path | complete)
tg --token $bob.token grant $carol.user.id node $sandbox
tg index
success (tg --token $carol.token sandbox get $sandbox | complete)
failure (tg --token $bob.token grant $carol.user.id parent $sandbox | complete)
tg --token $alice.token grant $bob.user.id parent $sandbox
tg index
success (tg --token $bob.token run $'--sandbox=($sandbox)' $path | complete)
tg --token $bob.token sandbox destroy $sandbox
