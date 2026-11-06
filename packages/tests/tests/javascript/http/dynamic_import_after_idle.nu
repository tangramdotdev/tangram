use ../../../util/test.nu *

# A dynamic import that reconnects after the connection's idle timeout must not leave the connection dependent on the JavaScript thread, which blocks while it loads the static imports of the imported module.
let local = server spawn --config { http: { idle_timeout: 0.25 } }

let path = artifact {
	tangram.ts: '
		export default async () => {
			await tg.sleep(1);
			const { value } = await import("./a.tg.ts");
			return value;
		};
	'
	a.tg.ts: '
		import { b } from "./b.tg.ts";
		export const value = b;
	'
	b.tg.ts: '
		export const b = 1;
	'
}

let output = timeout 30s tg build $path | complete
success $output
assert equal ($output.stdout | from json) 1
