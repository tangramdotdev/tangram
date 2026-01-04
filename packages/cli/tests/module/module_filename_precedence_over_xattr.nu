use ../../test.nu *

# Test that filename pattern takes precedence over xattr.
# A file named foo.tg.ts with xattr "js" should be detected as "ts".

let server = spawn

let path = artifact {
	"foo.tg.ts": (file --xattrs { "user.tangram.module": "js" } "export default () => 'test'")
}

let id = tg checkin ($path | path join "foo.tg.ts")
let obj = tg object get $id

assert ($obj | str contains '"module":"ts"') "filename pattern should override xattr"
