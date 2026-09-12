import { Authorization } from "./authorization.ts";
import type { Sync } from "./sync.ts";

export type Tokens = Record<string, Tokens.Entry>;

export namespace Tokens {
	export type Entry = {
		authorization?: Array<Authorization.Token> | null;
		sync?: Sync.Token | null;
	};

	export let clone = (tokens: Tokens | null | undefined): Tokens =>
		Object.fromEntries(
			Object.entries(tokens ?? {}).map(([location, entry]) => [
				location,
				cloneEntry(entry),
			]),
		);

	export let isEmpty = (tokens: Tokens): boolean =>
		Object.values(tokens).every(isEmptyEntry);

	export let local = (tokens: Tokens): Entry | null => tokens.local ?? null;

	export let withLocal = (entry: Entry | null): Tokens =>
		entry === null || isEmptyEntry(entry) ? {} : { local: cloneEntry(entry) };

	export let withoutAuthorization = (tokens: Tokens): Tokens => {
		let output: Tokens = {};
		for (let [location, entry] of Object.entries(tokens)) {
			if (entry.sync !== null && entry.sync !== undefined) {
				output[location] = { sync: entry.sync };
			}
		}
		return output;
	};

	export let inherit = (tokens: Tokens, parent: Tokens): void => {
		for (let [location, entry] of Object.entries(parent)) {
			let inherited = cloneEntry(tokens[location] ?? {});
			let authorization = inherited.authorization ?? [];
			for (let token of entry.authorization ?? []) {
				if (
					authorization.some((existing) =>
						Authorization.Token.covers(existing, token),
					)
				) {
					continue;
				}
				authorization = authorization.filter(
					(existing) => !Authorization.Token.covers(token, existing),
				);
				authorization.push(token);
			}
			if (authorization.length > 0) {
				inherited.authorization = authorization;
			}
			if (entry.sync !== undefined && entry.sync !== null) {
				inherited.sync ??= entry.sync;
			}
			if (!isEmptyEntry(inherited)) {
				tokens[location] = inherited;
			}
		}
	};

	let cloneEntry = (entry: Entry): Entry => ({
		...entry,
		...(entry.authorization === null || entry.authorization === undefined
			? {}
			: { authorization: [...entry.authorization] }),
	});

	let isEmptyEntry = (entry: Entry): boolean =>
		(entry.authorization?.length ?? 0) === 0 &&
		(entry.sync === null || entry.sync === undefined);
}
