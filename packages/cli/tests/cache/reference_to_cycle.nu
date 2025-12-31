use ../../test.nu *

let tmp = mktemp -d

let server = spawn

let artifact = artifact {
	tangram.ts: '
		export default () => {
			let graph = tg.graph({
				nodes: [
					{ kind: "directory", entries: { "b": 1 } },
					{ kind: "directory", entries: { "c": 2 } },
					{ kind: "file", dependencies: { "a": 0 } },
				]
			})
			return tg.directory({ graph, index: 1, kind: "directory" });
		}
	'
}
let id = tg build --no-cache-pointers $artifact

let output = tg cache $id

snapshot --path ($server.directory | path join "artifacts")
