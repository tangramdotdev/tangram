use ../lib/test.nu *

# A plain .ts file without xattr has no module kind.

let local = server spawn

let path = artifact {
	"foo.ts": "console.log('not a module')"
}

let id = tg checkin --no-tokens ($path | path join "foo.ts") | referent node
let obj = tg object get --no-tokens $id

snapshot --normalize-ids --redact $path $obj 'tg.file({"contents":blb_010000000000000000000000000000000000000000000000000000})'
