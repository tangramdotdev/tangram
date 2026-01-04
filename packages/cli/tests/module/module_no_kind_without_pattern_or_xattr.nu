use ../../test.nu *

# Test that a plain .ts file without xattr has no module kind.

let server = spawn

let path = artifact {
	"foo.ts": "console.log('not a module')"
}

let id = tg checkin ($path | path join "foo.ts")
let obj = tg object get $id

assert (not ($obj | str contains '"module"')) "plain .ts file should not have module kind"
