use ../../test.nu *

# --public reconciles a partial existing grant: if the process already has some public read permissions but not all, --public adds the missing ones without error.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

let path = artifact { tangram.ts: 'export default function () { return tg.file("partialsecret"); }' }

# Alice builds and gets the output file, then grants the public principal only the subtree (node), not the output.
let alice_build = tg --token $alice.token build --detach --verbose $path | from json
let file = (tg --token $alice.token wait $alice_build.process | from json).output.value | split row '?' | first
tg --token $alice.token grant public process_subtree $alice_build.process | ignore

# Eve cannot read the output object yet, since only the node is public.
let denied = tg --token $eve.token cat $file | complete
failure $denied "Eve should not read the output when only the node is public."

# Alice runs --public (cache hit on the partially-public process); it must add the missing read permissions without error.
let b2 = tg --token $alice.token build --detach --verbose --public $path | complete
success $b2 "--public must add missing permissions to a partially-public process without error."

# Now Eve can read the output.
let allowed = tg --token $eve.token cat $file | str trim
assert ($allowed == "partialsecret") "Eve should read the output after --public completes the public read."
