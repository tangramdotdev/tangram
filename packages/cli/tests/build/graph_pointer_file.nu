use ../../test.nu *

let server = spawn
let artifact = '
	tg.graph({
		"nodes": [
			{
				"kind": "file",
				"contents": tg.blob("export default () => \"Hello, World!\";"),
				"module": "ts"
			}
		]
	})
'
let graph_id = tg put $artifact | str trim
tg index
let output = tg get --blobs --depth=inf --pretty $graph_id | complete
success $output
snapshot ($output.stdout | str trim) '
	tg.graph({
	  "nodes": [
	    {
	      "kind": "file",
	      "contents": tg.blob("export default () => \"Hello, World!\";"),
	      "module": "ts",
	    },
	  ],
	})
'

let output = tg build $"graph=($graph_id)&index=0&kind=file" | complete
success $output
snapshot ($output.stdout | str trim) '"Hello, World!"'
