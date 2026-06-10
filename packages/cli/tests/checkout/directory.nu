use ../../test.nu *

# Checking out a directory with a single file entry materializes the directory with its file on disk.

let tmp = mktemp --directory

let server = spawn

let artifact = artifact {
	tangram.ts: '
		export default () => {
			return tg.directory({
				"hello.txt": "Hello, World!",
			})
		}
	'
}
let id = tg build $artifact

let path = $tmp | path join "checkout"
tg checkout $id $path
snapshot --path $path '
	{
	  "kind": "directory",
	  "entries": {
	    "hello.txt": {
	      "kind": "file",
	      "contents": "Hello, World!"
	    }
	  }
	}
'
