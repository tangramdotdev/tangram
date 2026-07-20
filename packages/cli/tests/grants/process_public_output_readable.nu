use ../../test.nu *

# A public build's output is readable by another owner: after reusing a public build, its output contents can be read.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

let path = artifact { tangram.ts: 'export default function () { return tg.file("publicsecret"); }' }

# Alice builds publicly.
let alice_build = tg --token $alice.token build --detach --verbose --public $path | from json
tg --token $alice.token wait $alice_build.process | complete

# Eve reuses it and reads the output contents.
let eve_build = tg --token $eve.token build --detach --verbose $path | from json
let eve_output = (tg --token $eve.token wait $eve_build.process | from json).output.value
let contents = tg --token $eve.token cat $eve_output | str trim
assert ($contents == "publicsecret") "Eve should read the public build's output contents."
