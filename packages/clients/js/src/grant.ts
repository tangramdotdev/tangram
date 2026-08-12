export namespace Grant {
	export type Token = string;
	export type Tokens = Record<string, Token>;

	export namespace Tokens {
		export let local = (tokens: Grant.Tokens): Grant.Token | null => {
			return tokens.local ?? null;
		};

		export let withLocal = (token: Grant.Token | null): Grant.Tokens => {
			return token === null ? {} : { local: token };
		};

		export let inherit = (tokens: Grant.Tokens, parent: Grant.Tokens): void => {
			for (let [location, token] of Object.entries(parent)) {
				tokens[location] ??= token;
			}
		};
	}
}
