use ../../test.nu *

# Tagging an object the tagger cannot read records no permissions, so the tag must not confer read access to that object.

let server = spawn --config { authentication: { providers: { insecure: true } } }
let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice builds a private file.
let alice_path = artifact { tangram.ts: 'export default function () { return tg.file("topsecret"); }' }
let alice_process = tg --token $alice.token build --detach $alice_path | str trim
let file = (tg --token $alice.token wait $alice_process | from json).output.value.id

# Eve cannot read Alice's private file.
let denied = tg --token $eve.token get $file | complete
failure $denied "Eve should not read Alice's private file before tagging it."

# Eve tags Alice's private file under her own namespace.
let tagged = tg --token $eve.token tag put eve/leak $file | complete
success $tagged "Eve should be able to create a tag in her own namespace."

# Eve must not gain read access to Alice's private file by tagging it.
let leaked = tg --token $eve.token get $file | complete
failure $leaked "Eve must not read Alice's private file after tagging it."
