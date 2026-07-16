use ../../test.nu *

# --public publicizes a process even on a cache hit: building privately and then --public the same command must make the output publicly readable.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

let path = artifact { tangram.ts: 'export default function () { return tg.file("laterpublic"); }' }

# Alice builds privately and gets the output file.
let alice_build = tg --token $alice.token build --detach --verbose $path | from json
let file = (tg --token $alice.token wait $alice_build.process | from json).output.value.id

# Eve cannot read the private output.
let before = tg --token $eve.token cat $file | complete
failure $before "Eve should not read Alice's private output before --public."

# Alice re-builds --public; this is a cache hit on her private process and must publicize it.
tg --token $alice.token build --detach --verbose --public $path | complete

# Now Eve can read the output.
let after = tg --token $eve.token cat $file | str trim
assert ($after == "laterpublic") "Eve should read the output after --public publicizes the cached process."
