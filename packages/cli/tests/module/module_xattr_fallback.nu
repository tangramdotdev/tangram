use ../../test.nu *

# Xattr is used when filename doesn't match a known pattern.
# A file named foo.ts (not .tg.ts) with xattr "ts" should be detected as "ts".

let server = spawn

let path = artifact {
	"foo.ts": (file --xattrs { "user.tangram.module": "ts" } "export default function () { return 'test'; }")
}

let id = tg checkin ($path | path join "foo.ts")
let obj = tg object get $id

snapshot ($obj | redact $path | normalize_ids) 'tg.file({"contents":blb_010000000000000000000000000000000000000000000000000000,"module":"ts"})'
