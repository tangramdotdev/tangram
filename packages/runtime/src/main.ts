import * as tg from "./index.ts";

let console = { log: tg.log, error: tg.error };
Object.defineProperties(globalThis, {
	console: {
		value: console,
		configurable: true,
		enumerable: true,
		writable: true,
	},
});

let Tangram = tg.template;
Object.assign(Tangram, tg);
Object.defineProperties(globalThis, {
	Tangram: { value: Tangram },
	tg: { value: Tangram },
});
