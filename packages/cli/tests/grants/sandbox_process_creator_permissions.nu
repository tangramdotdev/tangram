use ../lib/test.nu *

# The process creator receives parent authority independently of the sandbox creator.
let server = server spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let sandbox = tg --token $alice.token sandbox create | str trim
tg --token $alice.token grant $bob.user.id write $sandbox
let path = artifact { tangram.ts: 'export default () => tg.file("creator-access");' }
let process = tg --token $bob.token spawn $'--sandbox=($sandbox)' $path | str trim
tg --token $bob.token wait --source=index $process | ignore
success (tg --token $bob.token process get --source=index $process | complete)
failure (tg --token $alice.token process get --source=index $process | complete)

# Persisting the sandbox list must not confer process authority on its owner.
tg --token $alice.token sandbox destroy $sandbox
tg --token $alice.token sandbox wait --source=index $sandbox | ignore
failure (tg --token $alice.token process get --source=index $process | complete)
success (tg --token $bob.token process get --source=index $process | complete)

# The process creator can delegate access independently of the sandbox.
tg --token $bob.token grant $alice.user.id process_parent $process
success (tg --token $alice.token process get --source=index $process | complete)
