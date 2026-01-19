use ../../test.nu *

# Test building from a tag with ?path= when there's a circular import.
# tangram.ts re-exports from lib/a.tg.ts, and lib/a.tg.ts imports from tangram.ts.

let server = spawn

let path = artifact {
	tangram.ts: '
		export { x } from "./lib/a.tg.ts"
		export const y = 1;',
	lib: {
		a.tg.ts: '
			import { y } from "../tangram.ts"
			export const x = y'
	}
}
tg tag pkg $path
tg build 'pkg?path=lib/a.tg.ts#x'
