import * as tg from "./index.ts";

let console = { log: tg.log };
Object.defineProperties(globalThis, {
	console: {
		value: console,
		configurable: true,
		enumerable: true,
		writable: true,
	},
});

Object.defineProperties(globalThis, {
	tg: { value: tg },
});
