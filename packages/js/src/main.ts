import * as tg from "@tangramdotdev/client";
import { handle } from "./handle.ts";
import { error, log } from "./log.ts";
import { start } from "./start.ts";

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
Object.defineProperties(globalThis, {
	Tangram: { value: Tangram },
	tg: { value: Tangram },
});

Object.defineProperty(globalThis, "start", { value: start });

tg.setHandle(handle);

export const waitpid = async (process: number) => {
	return syscall("process_wait", process, {
		local: undefined,
		remotes: undefined,
		token: undefined,
	});
};
