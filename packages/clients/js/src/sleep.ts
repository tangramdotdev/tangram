import * as tg from "./index.ts";

export let sleep = async (duration: number) => {
	return await tg.handle.sleep(duration);
};
