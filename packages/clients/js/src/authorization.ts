import * as tg from "./index.ts";

export namespace Authorization {
	export type Token = string;

	export namespace Token {
		type Data = {
			body: {
				expires_at: bigint;
				permissions: Array<string>;
				resource: string;
			};
			metadata: {
				algorithm: string;
				key: string;
			};
		};

		const cache = new Map<string, Data | null>();

		// Compare the resources and permissions without considering expiration or verifying signatures.
		export let covers = (token: Token, other: Token): boolean => {
			if (token === other) {
				return true;
			}
			let a = parse(token);
			let b = parse(other);
			return (
				a !== null &&
				b !== null &&
				a.body.resource === b.body.resource &&
				b.body.permissions.every((needed) =>
					grants(token, b.body.resource, needed),
				)
			);
		};

		// Check the resource and implied permission without verifying the signature or expiration.
		export let grants = (
			token: Token,
			resource: string,
			permission: string,
		): boolean => {
			let data = parse(token);
			return (
				data !== null &&
				data.body.resource === resource &&
				data.body.permissions.some((granted) => implies(granted, permission))
			);
		};

		// Check object containment coverage through the same permission rules as other resource kinds.
		export let grantsObjectSubtree = (
			token: Token,
			resource: string,
		): boolean => grants(token, resource, "object_subtree");

		export let resource = (token: Token): string | null =>
			parse(token)?.body.resource ?? null;

		let parse = (token: Token): Data | null => {
			if (cache.has(token)) {
				return cache.get(token)!;
			}
			let output: Data | null = null;
			try {
				let parts = token.split(".");
				if (parts.length === 4 && parts[0] === "0") {
					let decode = (part: string) =>
						JSON.parse(
							tg.encoding.utf8.decode(tg.encoding.base64.decode(part)),
						);
					let bodyString = tg.encoding.utf8.decode(
						tg.encoding.base64.decode(parts[1]!),
					);
					let expiration = bodyString.match(/"expires_at"\s*:\s*(-?\d+)/g);
					if (expiration?.length !== 1) {
						return null;
					}
					let body = JSON.parse(
						bodyString.replace(/("expires_at"\s*:\s*)(-?\d+)/, '$1"$2"'),
					);
					body.expires_at = BigInt(body.expires_at);
					let metadata = decode(parts[2]!);
					if (
						body !== null &&
						typeof body === "object" &&
						typeof body.resource === "string" &&
						typeof body.expires_at === "bigint" &&
						body.expires_at >= -(1n << 63n) &&
						body.expires_at < 1n << 63n &&
						Array.isArray(body.permissions) &&
						body.permissions.every(
							(permission: unknown) => typeof permission === "string",
						) &&
						metadata !== null &&
						typeof metadata === "object" &&
						typeof metadata.algorithm === "string" &&
						typeof metadata.key === "string"
					) {
						output = { body, metadata };
					}
				}
			} catch {
				// Opaque tokens are retained for the server to validate.
			}
			if (cache.size >= 1024) {
				cache.clear();
			}
			cache.set(token, output);
			return output;
		};

		let implies = (granted: string, needed: string): boolean => {
			if (granted === needed) {
				return true;
			}
			if (needed === "object_node") {
				return granted === "object_subtree";
			}
			const process = [
				"process_node",
				"process_node_command",
				"process_node_error",
				"process_node_log",
				"process_node_output",
				"process_parent",
				"process_subtree",
				"process_subtree_command",
				"process_subtree_error",
				"process_subtree_log",
				"process_subtree_output",
			];
			if (process.includes(needed)) {
				return (
					granted === "process_parent" ||
					granted === needed.replace("process_node", "process_subtree")
				);
			}
			for (let kind of ["group", "organization", "sandbox", "tag", "user"]) {
				if (needed === `${kind}_read`) {
					return (
						granted === `${kind}_write` ||
						(kind !== "sandbox" && granted === `${kind}_admin`)
					);
				}
				if (needed === `${kind}_write`) {
					return kind !== "sandbox" && granted === `${kind}_admin`;
				}
			}
			return false;
		};
	}

	export type Tokens = Record<string, Tokens.Entry>;

	export namespace Tokens {
		export type Entry = Array<Authorization.Token>;

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
				entry === null || isEmptyEntry(entry)
					? {}
					: { local: cloneEntry(entry) };
			normalize(tokens);
			return tokens;
		};

		export let inherit = (
			tokens: Tokens,
			parent: Tokens,
			resource?: string,
		): void => {
			for (let [location, entry] of Object.entries(parent)) {
				tokens[location] = [...(tokens[location] ?? []), ...entry];
			}
			normalize(tokens, resource);
		};

		// Normalize each location independently, optionally pruning proofs redundant for the receiving object.
		export let normalize = (tokens: Tokens, resource?: string): void => {
			for (let [location, entry] of Object.entries(tokens)) {
				// Compare proofs only within the same resource, using the encoded token to break ties.
				let resources = new Map<string, Array<Authorization.Token>>();
				let authorization: Array<Authorization.Token> = [];
				for (let token of [...new Set(entry)].sort()) {
					let resource = Authorization.Token.resource(token);
					if (resource === null) {
						authorization.push(token);
						continue;
					}
					let proofs = resources.get(resource) ?? [];
					if (
						proofs.some((existing) =>
							Authorization.Token.covers(existing, token),
						)
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
				tokens[location] = authorization;
			}
		};

		let cloneEntry = (entry: Entry): Entry => [...entry];

		let isEmptyEntry = (entry: Entry): boolean => entry.length === 0;
	}
}
