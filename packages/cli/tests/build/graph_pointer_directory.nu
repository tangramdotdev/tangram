use ../../test.nu *

let server = spawn
let artifact = '
	tg.graph({
		"nodes": [
			{
				"kind": "directory",
				"entries": {
					"tangram.ts": { "index": 1, "kind": "file" },
				}
			},
			{
				"kind": "file",
				"contents": tg.blob("export default () => \"Hello, World!\";"),
				"module": "ts",
			}
		]
	})
'
let graph_id = tg put $artifact | str trim
let output = tg build $"graph=($graph_id)&index=0&kind=directory" | complete
success $output
snapshot ($output.stdout | str trim) '"Hello, World!"'
