use ../../test.nu *

let server = spawn --config { write: { cache_pointers: false } }

let path = artifact {
	tangram.ts: r#'
		export default () => {
			return tg.directory({
				graph: tg.graph({
					nodes: [
						{ kind: "directory", entries: { "bar": 1 } },
						{ kind: "directory", entries: { "foo": 0 } },
					]
				}),
				index: 0,
				kind: "directory",
			})
		}
	'#
}
let id = tg build $path

# Cache.
let output = tg cache $id | complete
failure $output

let stderr = $output.stderr | lines | last
snapshot $stderr '-> detected a directory cycle'
