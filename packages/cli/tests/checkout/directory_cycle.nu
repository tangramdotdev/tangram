use ../../test.nu *

let tmp = mktemp -d

let server = spawn

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

let path = $tmp | path join "checkout"
let output = tg checkout $id $path | complete

failure $output
let stderr = $output.stderr | lines | last
snapshot $stderr '-> detected a directory cycle'
