use ../lib/test.nu *

# A module with xattr (but no .tg.ts extension) can be built by ID.

let local = server spawn

let path = artifact {
	"module.ts": (file --xattrs { "user.tangram.module": "ts" } 'export default function () { return "xattr module"; }')
}

let id = tg checkin --no-tokens ($path | path join "module.ts") | referent node
tg index

let output = tg build $id
snapshot $output '"xattr module"'
