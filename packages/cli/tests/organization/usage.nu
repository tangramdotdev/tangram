use ../../test.nu *

# Organization usage is available by id and specifier.

let server = spawn --config {
	authentication: { users: { providers: { insecure: true } } },
	usage: true,
}
let alice = tg login --verbose --name alice | from json
let organization = tg organization create acme | from json

let by_id = tg organization usage $organization.id | from json
let by_specifier = tg usage acme | from json
assert ($by_id == $by_specifier)
assert ($by_id.object_count == 0)
assert ($by_id.object_size == 0)
assert ($by_id.process_count == 0)

let object = tg put 'tg.file("owned through a tag")' | str trim
tg tag put -p acme/owned $object
wait_until {
	(tg organization usage $organization.id | from json | get object_count) >= 1
} "the tag should add the object to the organization's storage"

let usage = tg organization usage $organization.id | from json
assert ($usage.object_count >= 1)
assert ($usage.object_size > 0)
