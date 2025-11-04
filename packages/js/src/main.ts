import * as tg from "@tangramdotdev/client";
import { error, log } from "./log.ts";
import { start } from "./start.ts";
import "./handle.ts";

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

Object.defineProperty(globalThis, "start", { value: start });
