import * as tg from "./index.ts";

export type Referent<T> = {
	item: T;
	path?: string | undefined;
	tag?: tg.Tag | undefined;
};

export namespace Referent {
	export type String<_T> = string;

	export let toString = <T>(value: tg.Referent<T>): tg.Referent.String<T> => {
		return tg.todo();
	};

	export let fromString = <T>(
		string: tg.Referent.String<T>,
	): tg.Referent<T> => {
		return tg.todo();
	};
}
