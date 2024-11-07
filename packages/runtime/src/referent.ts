import type * as tg from "./index.ts";

export type Referent<T> = {
	item: T;
	subpath?: string | undefined;
	tag?: tg.Tag | undefined;
};
