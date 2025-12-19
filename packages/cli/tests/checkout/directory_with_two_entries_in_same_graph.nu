use ../../test.nu *

let tmp = mktemp -d

let server = spawn

let artifact = artifact {
	tangram.ts: r#'
		export default () => {
			let baz = tg.file("hello, world!");
			let graph = tg.graph({
				nodes: [
					{ kind: "directory", entries: { "foo": 1 } },
					{ kind: "file", dependencies: {"../bar": 2, baz } },
					{ kind: "directory", entries: { "tangram.ts": 3 } },
					{ kind: "file", dependencies: { "../foo": 0 } },
				]
			});
			return tg.directory({
				foo: tg.directory({
					graph,
					index: 0,
					kind: "directory",
				}),
				bar: tg.directory({
					graph,
					index: 2,
					kind: "directory",
				})
			})
		}
	'#
}
let id = tg build $artifact

let path = $tmp | path join "checkout"
tg checkout $id $path
snapshot --path $path
