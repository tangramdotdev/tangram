export namespace Authorization {
	export type Token = string;
	export type Tokens = Record<string, Token>;

	export namespace Tokens {
		export let local = (
			tokens: Authorization.Tokens,
		): Authorization.Token | null => {
			return tokens.local ?? null;
		};

		export let withLocal = (
			token: Authorization.Token | null,
		): Authorization.Tokens => {
			return token === null ? {} : { local: token };
		};

		export let inherit = (
			tokens: Authorization.Tokens,
			parent: Authorization.Tokens,
		): void => {
			for (let [location, token] of Object.entries(parent)) {
				tokens[location] ??= token;
			}
		};
	}
}
