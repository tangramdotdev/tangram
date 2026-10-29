use ../../../lib/test.nu *

# A JavaScript build that creates an object with more than 1,024 children or more than 1 MiB of data fails before storing the object.

let local = server spawn

let path = artifact {
	tangram.ts: '
		export let children = async () => {
			let dependencies: Record<string, tg.File> = {};
			for (let i = 0; i < 1025; i++) {
				dependencies[`./${i}`] = await tg.file(String(i));
			}
			return tg.file({ contents: "", dependencies });
		};

		export let size = () => tg.symlink("x".repeat(2 * 1024 * 1024));
	'
}

let output = tg build $'($path)#children' | complete
failure $output 'a file with 1,025 dependencies must be rejected'
assert ($output.stderr | str contains 'the object has too many children') 'the error must name the children limit'

let output = tg build $'($path)#size' | complete
failure $output 'a symlink with a 2 MiB path must be rejected'
assert ($output.stderr | str contains 'the object is too large') 'the error must name the size limit'
