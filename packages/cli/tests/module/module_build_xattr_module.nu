use ../../test.nu *

# Test that a module with xattr (but no .tg.ts extension) can be built by ID.

let server = spawn

let path = artifact {
	"module.ts": (file --xattrs { "user.tangram.module": "ts" } 'export default () => "xattr module"')
}

let id = tg checkin ($path | path join "module.ts")
tg index

let output = tg build $id
snapshot $output '"xattr module"'
