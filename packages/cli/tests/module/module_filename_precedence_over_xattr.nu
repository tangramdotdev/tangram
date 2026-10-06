use ../lib/test.nu *

# Filename pattern takes precedence over xattr.
# A file named foo.tg.ts with xattr "javascript" should be detected as "typescript".

let local = server spawn

let path = artifact {
	"foo.tg.ts": (file --xattrs { "user.tangram.module": "javascript" } "export default function () { return 'test'; }")
}

let id = tg checkin --no-tokens ($path | path join "foo.tg.ts") | referent node
let obj = tg object get --no-tokens $id

snapshot --normalize-ids --redact $path $obj 'tg.file({"contents":blb_010000000000000000000000000000000000000000000000000000,"module":"typescript"})'
