export namespace Grant {
	export type Token = string;

	export type WithToken<T> = {
		id: T;
		token: Token;
	};

	export type MaybeWithToken<T> = T | WithToken<T>;
}
