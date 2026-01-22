use ../../test.nu *

let server = spawn
let path = artifact {
	foo.tg.ts: ''
	bar.tg.ts: '
		import "foo"
	'
}

tg tag foo ($path | path join 'foo.tg.ts')

let id = tg checkin --destructive --ignore=false --lock=file ($path | path join 'bar.tg.ts')
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot $object '
	tg.file({
	  "contents": tg.blob("import \"foo\""),
	  "dependencies": {
	    "foo": {
	      "item": tg.file({
	        "contents": tg.blob(""),
	        "module": "ts",
	      }),
	      "options": {
	        "id": "fil_01errf3d75w4h6hc0ewgx04jz11cd996405b4tajcgksa8krs5k2q0",
	        "tag": "foo",
	      },
	    },
	  },
	  "module": "ts",
	})
'

let lockfile_path = $path | path join 'bar.tg.lock'
assert (not ($lockfile_path | path exists))

let xattrs = xattr_list ($path | path join 'bar.tg.ts') | where { |name| $name == 'user.tangram.lock' }
assert ($xattrs | is-empty)
