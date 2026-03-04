use ../../test.nu *

# When --checkout is set and the output is a directory, `tg run` should check out the directory.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => tg.directory({
			"hello.txt": tg.file("hello"),
			"world.txt": tg.file("world"),
		});
	'
}

let sandbox_output = tg run --checkout $path --sandbox | str trim
print $sandbox_output
print (file $sandbox_output)
assert ($sandbox_output | path exists)
assert (($sandbox_output | path join "hello.txt") | path exists)
let sandbox_contents = open ($sandbox_output | path join "hello.txt")
assert ($sandbox_contents == "hello")

# let output = tg run --checkout $path | str trim
# assert ($output | path exists)
# assert (($output | path join "hello.txt") | path exists)
# let contents = open ($output | path join "hello.txt")
# assert ($contents == "hello")
