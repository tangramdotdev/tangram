use ../../test.nu *

# Filename pattern takes precedence over xattr.
# A file named foo.tg.ts with xattr "js" should be detected as "ts".

let server = spawn

let path = artifact {
	"foo.tg.ts": (file --xattrs { "user.tangram.module": "js" } "export default function () { return 'test'; }")
}

let id = tg checkin ($path | path join "foo.tg.ts")
let obj = tg object get $id

snapshot --normalize-ids --redact $path $obj 'tg.file({"contents":blb_010000000000000000000000000000000000000000000000000000,"module":"ts"})'
