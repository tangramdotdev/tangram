use ../../util/test.nu *

# Modules in a cross-language import cycle check against each other's types without waiting on each other.

let local = server spawn

let path = artifact {
	tangram.ts: '
		import { bake } from "./bakery.tg.py";
		export async function decorate(order: tg.File, flavor: string) {
			return tg.file(`${await order.text} with ${flavor} icing`);
		}
		export default async function () {
			const cake: tg.File = await bake.build(await tg.file("moon cake"));
			return cake.text;
		}
	'
	bakery.tg.py: '
		# /// script
		# [tool.tangram.imports.root]
		# specifier = "./tangram.ts"
		# ///
		from root import decorate
		async def bake(order: tg.File) -> tg.File:
		    return await decorate.build(order, "stardust")
	'
}
success (tg check $path | complete)
success (tg check ($path | path join bakery.tg.py) | complete)
let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json) 'moon cake with stardust icing'

let path = artifact {
	tangram.ts: '
		import { bake } from "./bakery.tg.py";
		export async function decorate(order: tg.File, flavor: string) {
			return tg.file(`${await order.text} with ${flavor} icing`);
		}
		export default async function () {
			await bake.build("moon cake");
		}
	'
	bakery.tg.py: '
		# /// script
		# [tool.tangram.imports.root]
		# specifier = "./tangram.ts"
		# ///
		from root import decorate
		async def bake(order: tg.File) -> tg.File:
		    return await decorate.build(order, 42)
	'
}
let output = tg check $path | complete
failure $output
assert ($output.stderr | str contains 'tangram.ts:6:') $output.stderr
let output = tg check ($path | path join bakery.tg.py) | complete
failure $output
assert ($output.stderr | str contains 'bakery.tg.py:7:') $output.stderr
