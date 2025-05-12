import * as tg from "./index.ts";
import { error, log } from "./log.ts";

Object.defineProperties(globalThis, {
	console: {
		value: { error, log },
		configurable: true,
		enumerable: true,
		writable: true,
	},
});

let Tangram = tg.template;
Object.assign(Tangram, tg);
Object.defineProperty(Tangram, "process", { get: () => tg.Process.current });
Object.defineProperties(globalThis, {
	Tangram: { value: Tangram },
	tg: { value: Tangram },
});
