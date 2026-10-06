import * as tg from "./index.ts";

/** Sleep for the specified duration in seconds. */
export let sleep = async (duration: number) => {
	return await tg.host.sleep(duration);
};
