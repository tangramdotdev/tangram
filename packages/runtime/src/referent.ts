import type * as tg from "./index.ts";

export type Referent<T> = {
	item: T;
	path?: string | undefined;
	subpath?: string | undefined;
	tag?: tg.Tag | undefined;
};
