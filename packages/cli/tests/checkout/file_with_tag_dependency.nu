use ../../test.nu *

# Checking out a file with a tagged dependency, with dependencies disabled, materializes the file with the lock recorded in its user.tangram.lock xattr.

let tmp = mktemp --directory

let server = spawn

let artifact = artifact {
	tangram.ts: '
		export default async function () {
			let bar = await tg.file("bar");
			return tg.file({
				contents: "foo",
				dependencies: {
					"bar": {
						item: bar,
						options: {
							id: bar.id,
							tag: "bar"
						}
					}
				}
			})
		}
	'
}
let id = tg build $artifact

let path = $tmp | path join "checkout"
tg checkout --dependencies=false $id $path
snapshot --path $path '
	{
	  "kind": "file",
	  "contents": "foo",
	  "xattrs": {
	    "user.tangram.dependencies": "[\"bar\"]",
	    "user.tangram.lock": "{\"nodes\":[{\"kind\":\"file\",\"dependencies\":{\"bar\":{\"item\":null,\"options\":{\"id\":\"fil_01drxezv07bnpqt9w6jw4hqrc73b1n66y19krh1krscbc307124z2g\",\"tag\":\"bar\"}}}}]}"
	  }
	}
'

let lock = xattr_read "user.tangram.lock" $path | from json | to json
snapshot $lock '
	{
	  "nodes": [
	    {
	      "kind": "file",
	      "dependencies": {
	        "bar": {
	          "item": null,
	          "options": {
	            "id": "fil_01drxezv07bnpqt9w6jw4hqrc73b1n66y19krh1krscbc307124z2g",
	            "tag": "bar"
	          }
	        }
	      }
	    }
	  ]
	}
'
