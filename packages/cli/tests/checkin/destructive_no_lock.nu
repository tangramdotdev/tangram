use ../../test.nu *

# A destructive checkin with --lock=file does not write a lockfile or lock xattr for a file with a tag dependency.

let server = server spawn
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
	      "node": tg.file({
	        "contents": tg.blob(""),
	        "module": "ts",
	      }),
	      "options": {
	        "id": "fil_01pt07w6c61hepxc7n76nskysszbn4n3g8m2xscm1d9tny4myfy9qg",
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
