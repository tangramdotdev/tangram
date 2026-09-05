use ../../test.nu *

# Increasing dependency counts must not let optional tokens crowd out required module and lock xattrs.

let server = server spawn
let tmp = mktemp --directory

for count in 1..8 {
	let source = '
		export default async function () {
			const dependency = await tg.file("dependency");
			const dependencies = {};
			for (let index = 0; index < COUNT; index++) {
				const name = `dependency${index}`;
				dependencies[name] = {
					node: dependency,
					options: { id: dependency.id, tag: name },
				};
			}
			return tg.file({ contents: "input", dependencies, module: "ts" });
		}
	' | str replace 'COUNT' ($count | into string)
	let id = tg build (artifact { tangram.ts: $source })
	let path = $tmp | path join ($count | into string)
	tg checkout --lock=attr --dependencies=false $id --path $path

	assert equal (xattr_read 'user.tangram.module' $path) 'ts'
	let lock = xattr_read 'user.tangram.lock' $path | from json
	assert equal ($lock.nodes | first | get dependencies | columns | length) $count

	let names = xattr_list $path | where { |name| $name starts-with 'user.tangram.dependencies' }
	let names = if 'user.tangram.dependencies' in $names {
		$names
	} else {
		$names | sort-by --custom { |left, right|
			($left | split row '.' | last | into int) < ($right | split row '.' | last | into int)
		}
	}
	let dependencies = $names | each { |name| xattr_read $name $path } | str join | from json
	assert equal ($dependencies | length) $count
}
