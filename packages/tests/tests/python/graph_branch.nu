use ../../lib/test.nu *

# tg.graph in Python with nodes that have more than 1,024 children splits the nodes into leaves and a branch, and the graph's artifacts can still be read.

let local = server spawn

let path = artifact {
	'test.tg.py': '
		async def default():
			entries = {f"file_{i}": i + 1 for i in range(1100)}
			nodes = [{"kind": "directory", "entries": entries}]
			for i in range(1100):
				nodes.append({"kind": "file", "contents": await tg.blob(f"file {i}")})
			graph = await tg.graph({"nodes": nodes})
			value = await graph.object()
			directory = await graph.get(0)
			file = await directory.get("file_1099")
			return [len(value["children"]), len(await graph.nodes()), await file.text()]
	'
}
let output = tg build ($path | path join test.tg.py)
snapshot $output '[2,1101,"file 1099"]'
