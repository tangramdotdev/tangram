import { Authorization } from "./authorization.ts";
import type { Sync } from "./sync.ts";

export type Tokens = Record<string, Tokens.Entry>;

export namespace Tokens {
	export type Entry = {
		authorization?: Array<Authorization.Token> | null;
		sync?: Array<Sync.Token> | null;
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

	export let withLocal = (entry: Entry | null): Tokens => {
		let tokens =
			entry === null || isEmptyEntry(entry) ? {} : { local: cloneEntry(entry) };
		normalize(tokens);
		return tokens;
	};

	export let withoutAuthorization = (tokens: Tokens): Tokens => {
		let output: Tokens = {};
		for (let [location, entry] of Object.entries(tokens)) {
			if ((entry.sync?.length ?? 0) > 0) {
				output[location] = { sync: [...entry.sync!] };
			}
		}
		return output;
	};

	export let inherit = (
		tokens: Tokens,
		parent: Tokens,
		resource?: string,
	): void => {
		for (let [location, entry] of Object.entries(parent)) {
			let inherited = cloneEntry(tokens[location] ?? {});
			inherited.authorization = [
				...(inherited.authorization ?? []),
				...(entry.authorization ?? []),
			];
			inherited.sync = [...(inherited.sync ?? []), ...(entry.sync ?? [])];
			tokens[location] = inherited;
		}
		normalize(tokens, resource);
	};

	// Normalize each location independently, optionally pruning proofs redundant for the receiving object.
	export let normalize = (tokens: Tokens, resource?: string): void => {
		for (let [location, entry] of Object.entries(tokens)) {
			let authorization: Array<Authorization.Token> = [];
			// Compare later expirations first and break ties by the encoded token.
			const ordered = (entry.authorization ?? []).toSorted((a, b) => {
				const aExpiration = Authorization.Token.expiresAt(a);
				const bExpiration = Authorization.Token.expiresAt(b);
				if (aExpiration !== bExpiration) {
					if (aExpiration === null) return 1;
					if (bExpiration === null) return -1;
					return aExpiration > bExpiration ? -1 : 1;
				}
				return a < b ? -1 : a > b ? 1 : 0;
			});
			for (let token of ordered) {
				if (
					authorization.some(
						(existing) =>
							Authorization.Token.covers(existing, token) &&
							(!Authorization.Token.covers(token, existing) ||
								Authorization.Token.expiresAt(existing)! >
									Authorization.Token.expiresAt(token)! ||
								(Authorization.Token.expiresAt(existing) ===
									Authorization.Token.expiresAt(token) &&
									existing <= token)),
					)
				) {
					continue;
				}
				authorization = authorization.filter(
					(existing) => !Authorization.Token.covers(token, existing),
				);
				authorization.push(token);
			}
			// An exact subtree proof covers the receiving object regardless of the inherited proof's resource.
			if (resource !== undefined) {
				const proofs = authorization.filter((token) =>
					Authorization.Token.grantsObjectSubtree(token, resource),
				);
				authorization = authorization.filter(
					(token) =>
						Authorization.Token.grantsObjectSubtree(token, resource) ||
						!proofs.some((proof) =>
							Authorization.Token.coversObjectSubtree(proof, token, resource),
						),
				);
			}
			const sync = [...new Set(entry.sync ?? [])];
			if (authorization.length === 0 && sync.length === 0) {
				delete tokens[location];
				continue;
			}
			tokens[location] = {
				...(authorization.length === 0 ? {} : { authorization }),
				...(sync.length === 0 ? {} : { sync }),
			};
		}
	};

	let cloneEntry = (entry: Entry): Entry => ({
		...entry,
		...(entry.sync === null || entry.sync === undefined
			? {}
			: { sync: [...entry.sync] }),
		...(entry.authorization === null || entry.authorization === undefined
			? {}
			: { authorization: [...entry.authorization] }),
	});

	let isEmptyEntry = (entry: Entry): boolean =>
		(entry.authorization?.length ?? 0) === 0 && (entry.sync?.length ?? 0) === 0;
}
