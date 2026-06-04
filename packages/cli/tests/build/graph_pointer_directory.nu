use ../../test.nu *

# A graph pointer referencing a directory node can be built and resolves to the default export of the module file inside that directory.

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
