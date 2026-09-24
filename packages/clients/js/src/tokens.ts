import { Authorization } from "./authorization.ts";

export type Tokens = Record<string, Tokens.Entry>;

export namespace Tokens {
	export type Entry = {
		authorization?: Array<Authorization.Token> | null;
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
			tokens[location] = inherited;
		}
		normalize(tokens, resource);
	};

	// Normalize each location independently, optionally pruning proofs redundant for the receiving object.
	export let normalize = (tokens: Tokens, resource?: string): void => {
		for (let [location, entry] of Object.entries(tokens)) {
			// Compare proofs only within the same resource, using the encoded token to break ties.
			let resources = new Map<string, Array<Authorization.Token>>();
			let authorization: Array<Authorization.Token> = [];
			for (let token of [...new Set(entry.authorization ?? [])].sort()) {
				let resource = Authorization.Token.resource(token);
				if (resource === null) {
					authorization.push(token);
					continue;
				}
				let proofs = resources.get(resource) ?? [];
				if (
					proofs.some((existing) => Authorization.Token.covers(existing, token))
				) {
					continue;
				}
				proofs = proofs.filter(
					(existing) => !Authorization.Token.covers(token, existing),
				);
				proofs.push(token);
				resources.set(resource, proofs);
			}
			for (let proofs of resources.values()) {
				for (let token of proofs) {
					authorization.push(token);
				}
			}
			// Keep sync tokens so readers can wait for objects that are still being transferred.
			if (
				resource !== undefined &&
				authorization.some((token) =>
					Authorization.Token.grantsObjectSubtree(token, resource),
				)
			) {
				authorization = authorization.filter(
					(token) =>
						Authorization.Token.resource(token)?.startsWith("syn_") ||
						Authorization.Token.grantsObjectSubtree(token, resource),
				);
			}
			authorization.sort();
			if (authorization.length === 0) {
				delete tokens[location];
				continue;
			}
			tokens[location] = { authorization };
		}
	};

	let cloneEntry = (entry: Entry): Entry => ({
		...entry,
		...(entry.authorization === null || entry.authorization === undefined
			? {}
			: { authorization: [...entry.authorization] }),
	});

	let isEmptyEntry = (entry: Entry): boolean =>
		(entry.authorization?.length ?? 0) === 0;
}
