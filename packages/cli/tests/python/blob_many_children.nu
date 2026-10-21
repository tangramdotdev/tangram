use ../lib/test.nu *

# tg.blob in Python with more children than fit in a branch nests them in branches, like a blob written by checkin.

let local = server spawn

let path = artifact {
	'test.tg.py': '
		async def default():
			children = [await tg.blob(str(i)) for i in range(1025)]
			blob = await tg.blob(*children)
			value = await blob.object()
			return [len(value["children"]), await blob.text() == "".join(str(i) for i in range(1025))]
	'
}
let output = tg build ($path | path join test.tg.py)
snapshot $output '[2,true]'
