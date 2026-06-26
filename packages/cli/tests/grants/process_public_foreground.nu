use ../../test.nu *

# --public does not require --detach: a foreground public build is reusable by another owner.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

let path = artifact { tangram.ts: 'export default function () { return tg.file("publicsecret"); }' }

# Alice builds publicly in the foreground, without --detach.
tg --token $alice.token build --public $path | complete

# Eve building the identical command reuses Alice's public cached process.
let eve_build = tg --token $eve.token build --detach --verbose $path | from json
tg --token $eve.token wait $eve_build.process | complete
assert (($eve_build.cached? | default false) == true) "Eve should reuse Alice's foreground public build."
