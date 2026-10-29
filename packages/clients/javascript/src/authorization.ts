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
					authorizes(token, b.body.resource, needed),
				)
			);
		};

		// Check the resource and implied permission without verifying the signature or expiration.
		export let authorizes = (
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
		export let authorizesObjectSubtree = (
			token: Token,
			resource: string,
		): boolean => authorizes(token, resource, "object_subtree");

		export let resource = (token: Token): string | null =>
			parse(token)?.body.resource ?? null;

		export let expiresAt = (token: Token): bigint | null =>
			parse(token)?.body.expires_at ?? null;

		let parse = (token: Token): Data | null => {
			if (cache.has(token)) {
				return cache.get(token)!;
			}
			let output: Data | null = null;
			try {
				output = { body: decode(token) };
			} catch {
				// Opaque tokens are retained for the server to validate.
			}
			if (cache.size >= 1024) {
				cache.clear();
			}
			cache.set(token, output);
			return output;
		};

		// Decode the body that precedes the secret ID and the signature.
		let decode = (token: Token): Data["body"] => {
			let bytes = decodeBase32(token);
			let position = 0;
			if (bytes[position++] !== 0) {
				throw new Error("unsupported token version");
			}
			let expiration: bigint;
			[expiration, position] = readUvarint(bytes, position);
			if (expiration >= 1n << 63n) {
				throw new Error("invalid expiration");
			}
			let mask: bigint;
			[mask, position] = readUvarint(bytes, position);
			let resource: string;
			let kind: string;
			[resource, kind, position] = readId(bytes, position);
			let names = permissionNames[kind];
			if (names === undefined) {
				throw new Error("invalid resource");
			}
			let permissions: Array<string> = [];
			for (let id = 0; id < 64; id++) {
				if ((mask >> BigInt(id)) & 1n) {
					let name = names[id];
					if (name === undefined) {
						throw new Error("invalid permissions");
					}
					permissions.push(`${kind}_${name}`);
				}
			}
			[, position] = readUvarint(bytes, position);
			if (position >= bytes.length) {
				throw new Error("missing the signature");
			}
			return { expires_at: expiration, permissions, resource };
		};

		const symbols = "0123456789abcdefghjkmnpqrstvwxyz";

		let decodeBase32 = (value: string): Array<number> => {
			let bytes: Array<number> = [];
			let buffer = 0;
			let bits = 0;
			for (let character of value) {
				let symbol = symbols.indexOf(character);
				if (symbol === -1) {
					throw new Error("invalid symbol");
				}
				buffer = ((buffer << 5) | symbol) & 0xfff;
				bits += 5;
				if (bits >= 8) {
					bits -= 8;
					bytes.push((buffer >> bits) & 0xff);
				}
			}
			if (bits >= 5 || (buffer & ((1 << bits) - 1)) !== 0) {
				throw new Error("invalid trailing bits");
			}
			return bytes;
		};

		let encodeBase32 = (bytes: Array<number>): string => {
			let output = "";
			let buffer = 0;
			let bits = 0;
			for (let byte of bytes) {
				buffer = ((buffer << 8) | byte) & 0xfff;
				bits += 8;
				while (bits >= 5) {
					bits -= 5;
					output += symbols[(buffer >> bits) & 31];
				}
			}
			if (bits > 0) {
				output += symbols[(buffer << (5 - bits)) & 31];
			}
			return output;
		};

		let readUvarint = (
			bytes: Array<number>,
			position: number,
		): [bigint, number] => {
			let value = 0n;
			for (let shift = 0n; shift < 64n; shift += 7n) {
				let byte = bytes[position++];
				if (byte === undefined) {
					throw new Error("truncated varint");
				}
				value |= BigInt(byte & 0x7f) << shift;
				if ((byte & 0x80) === 0) {
					if (value >= 1n << 64n) {
						throw new Error("varint overflow");
					}
					return [value, position];
				}
			}
			throw new Error("varint overflow");
		};

		// Read a binary ID and return it with its resource kind.
		let readId = (
			bytes: Array<number>,
			position: number,
		): [string, string, number] => {
			let [version, , kind, algorithm] = bytes.slice(position, position + 4);
			let prefix = kind === undefined ? undefined : idPrefixes[kind];
			let length = algorithm === 0 ? 16 : algorithm === 1 ? 32 : undefined;
			if (version !== 0 || prefix === undefined || length === undefined) {
				throw new Error("invalid ID");
			}
			let body = bytes.slice(position + 4, position + 4 + length);
			if (body.length !== length) {
				throw new Error("invalid ID");
			}
			let id = `${prefix}_0${algorithm}${encodeBase32(body)}`;
			let resourceKind = resourceKinds[prefix] ?? "";
			return [id, resourceKind, position + 4 + length];
		};

		const idPrefixes: Record<number, string> = {
			0: "blb",
			1: "dir",
			2: "fil",
			3: "sym",
			4: "gph",
			5: "cmd",
			6: "err",
			7: "sbx",
			8: "pcs",
			9: "usr",
			10: "grp",
			11: "org",
			12: "tag",
			13: "rnr",
			14: "sch",
			15: "tok",
			16: "idx",
			17: "syn",
		};

		const resourceKinds: Record<string, string> = {
			blb: "object",
			cmd: "object",
			dir: "object",
			err: "object",
			fil: "object",
			gph: "object",
			grp: "group",
			org: "organization",
			pcs: "process",
			sbx: "sandbox",
			sym: "object",
			syn: "sync",
			tag: "tag",
			usr: "user",
		};

		// List each resource kind's permissions in the order of their IDs.
		const permissionNames: Record<string, Array<string>> = {
			group: ["admin", "read", "write"],
			object: ["node", "subtree"],
			organization: ["admin", "read", "write"],
			process: [
				"node",
				"node_command_objects",
				"node_error_objects",
				"node_log_objects",
				"node_output_objects",
				"parent",
				"subtree",
				"subtree_command_objects",
				"subtree_error_objects",
				"subtree_log_objects",
				"subtree_output_objects",
			],
			sandbox: ["node", "parent"],
			sync: ["read"],
			tag: ["admin", "read", "write"],
			user: ["admin", "read", "write"],
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
				"process_node_command_objects",
				"process_node_error_objects",
				"process_node_log_objects",
				"process_node_output_objects",
				"process_parent",
				"process_subtree",
				"process_subtree_command_objects",
				"process_subtree_error_objects",
				"process_subtree_log_objects",
				"process_subtree_output_objects",
			];
			if (process.includes(needed)) {
				return (
					granted === "process_parent" ||
					granted === needed.replace("process_node", "process_subtree")
				);
			}
			if (needed === "sandbox_node") {
				return granted === "sandbox_parent";
			}
			for (let kind of ["group", "organization", "tag", "user"]) {
				if (needed === `${kind}_read`) {
					return granted === `${kind}_write` || granted === `${kind}_admin`;
				}
				if (needed === `${kind}_write`) {
					return granted === `${kind}_admin`;
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
				// Compare proofs only within the same resource, so that among equivalent proofs the one that expires last is kept, with the encoded token breaking remaining ties.
				let resources = new Map<string, Array<Authorization.Token>>();
				let authorization: Array<Authorization.Token> = [];
				for (let token of [...new Set(entry)].sort(compare)) {
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
				// Keep authorization tokens for syncs so readers can wait for objects that are still being transferred.
				if (
					resource !== undefined &&
					authorization.some((token) =>
						Authorization.Token.authorizesObjectSubtree(token, resource),
					)
				) {
					authorization = authorization.filter(
						(token) =>
							Authorization.Token.resource(token)?.startsWith("syn_") ||
							Authorization.Token.authorizesObjectSubtree(token, resource),
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

		let compare = (a: Authorization.Token, b: Authorization.Token): number => {
			let x = Authorization.Token.expiresAt(a) ?? -1n;
			let y = Authorization.Token.expiresAt(b) ?? -1n;
			if (x !== y) {
				return x > y ? -1 : 1;
			}
			return a < b ? -1 : a > b ? 1 : 0;
		};

		let cloneEntry = (entry: Entry): Entry => [...entry];

		let isEmptyEntry = (entry: Entry): boolean => entry.length === 0;
	}
}
