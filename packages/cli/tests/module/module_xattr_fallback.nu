use ../../test.nu *

# Test that xattr is used when filename doesn't match a known pattern.
# A file named foo.ts (not .tg.ts) with xattr "ts" should be detected as "ts".

let server = spawn

let path = artifact {
	"foo.ts": (file --xattrs { "user.tangram.module": "ts" } "export default () => 'test'")
}

let id = tg checkin ($path | path join "foo.ts")
let obj = tg object get $id

assert ($obj | str contains '"module":"ts"') "xattr should be used when filename doesn't match"
