use ../lib/test.nu *

# Xattr is used when filename doesn't match a known pattern.
# A file named foo.ts (not .tg.ts) with xattr "typescript" should be detected as "typescript".

let local = server spawn

let path = artifact {
	"foo.ts": (file --xattrs { "user.tangram.module": "typescript" } "export default function () { return 'test'; }")
}

let id = tg checkin --no-tokens ($path | path join "foo.ts") | referent node
let obj = tg object get --no-tokens $id

snapshot --normalize-ids --redact $path $obj 'tg.file({"contents":blb_010000000000000000000000000000000000000000000000000000,"module":"typescript"})'
