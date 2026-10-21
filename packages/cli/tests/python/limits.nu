use ../lib/test.nu *

# A Python build that creates an object with more than 1,024 children or more than 1 MiB of data fails before storing the object.

let local = server spawn

let path = artifact {
	'test.tg.py': '
		async def children():
			dependencies = {f"./{i}": await tg.file(str(i)) for i in range(1025)}
			return await tg.file({"contents": "", "dependencies": dependencies})

		async def size():
			return await tg.symlink("x" * (2 * 1024 * 1024))
	'
}
let module = $path | path join test.tg.py

let output = tg build $'($module)#children' | complete
failure $output 'a file with 1,025 dependencies must be rejected'
assert ($output.stderr | str contains 'the object has too many children') 'the error must name the children limit'

let output = tg build $'($module)#size' | complete
failure $output 'a symlink with a 2 MiB path must be rejected'
assert ($output.stderr | str contains 'the object is too large') 'the error must name the size limit'
