export namespace Authorization {
	export class Token {
		readonly body: Body | null;
		readonly secretId: bigint | null;
		readonly signature: ReadonlyArray<number>;
		#data: string;

		private constructor(
			data: string,
			body: Body | null,
			secretId: bigint | null,
			signature: ReadonlyArray<number>,
		) {
			if (body !== null) {
				Object.freeze(body.permissions);
				Object.freeze(body);
			}
			this.body = body;
			this.secretId = secretId;
			this.signature = Object.freeze(signature);
			this.#data = data;
			Object.freeze(this);
		}

		static fromData(data: string): Token {
			try {
				let { body, secretId, signature } = decode(data);
				return new Token(data, body, secretId, signature);
			} catch {
				// Retain opaque tokens for the server to validate.
				return new Token(data, null, null, []);
			}
		}

		toData(): string {
			return this.#data;
		}

		// Compare the resources and permissions without verifying signatures or expiration.
		covers(other: Token): boolean {
			let a = this.body;
			let b = other.body;
			return (
				this.#data === other.#data ||
				(a !== null &&
					b !== null &&
					a.resource === b.resource &&
					b.permissions.every((permission) =>
						this.authorizes(b.resource, permission),
					))
			);
		}

		// Check the resource and implied permission without verifying the signature or expiration.
		authorizes(resource: string, permission: string): boolean {
			return (
				this.body !== null &&
				this.body.resource === resource &&
				this.body.permissions.some((granted) => implies(granted, permission))
			);
		}

		authorizesObjectSubtree(resource: string): boolean {
			return this.authorizes(resource, "object_subtree");
		}
	}

	export namespace Token {
		export type Data = string;
	}

	export type Body = {
		readonly expiresAt: bigint;
		readonly permissions: ReadonlyArray<string>;
		readonly resource: string;
	};

	export class Tokens {
		#map: Map<string, Tokens.Entry>;

		constructor() {
			this.#map = new Map();
		}

		static fromData(data: Tokens | Tokens.Data | null | undefined): Tokens {
			if (data instanceof Tokens) {
				return data.clone();
			}
			let tokens = new Tokens();
			for (let [location, entry] of Object.entries(data ?? {})) {
				let authorization = entry.map((token) => Token.fromData(token));
				tokens.#map.set(location, new Tokens.Entry(authorization));
			}
			return tokens;
		}

		toData(): Tokens.Data {
			return Object.fromEntries(
				[...this.#map].map(([location, entry]) => [
					location,
					entry.authorization.map((token) => token.toData()),
				]),
			);
		}

		clone(): Tokens {
			let tokens = new Tokens();
			for (let [location, entry] of this.#map) {
				tokens.#map.set(location, new Tokens.Entry([...entry.authorization]));
			}
			return tokens;
		}

		iter(): IterableIterator<[string, Tokens.Entry]> {
			return this.#map.entries();
		}

		get(location: string): Tokens.Entry | undefined {
			return this.#map.get(location);
		}

		isEmpty(): boolean {
			return [...this.#map.values()].every((entry) => entry.isEmpty());
		}

		set(location: string, entry: Tokens.Entry): void {
			entry.normalize();
			this.#map.set(location, entry);
		}

		inherit(parent: Tokens, resource?: string): void {
			for (let [location, entry] of parent.iter()) {
				let authorization = [
					...(this.get(location)?.authorization ?? []),
					...entry.authorization,
				];
				this.#map.set(location, new Tokens.Entry(authorization));
			}
			this.normalize(resource);
		}

		// Normalize each location independently, optionally pruning proofs redundant for the receiving object.
		normalize(resource?: string): void {
			for (let [location, entry] of this.#map) {
				entry.normalize(resource);
				if (entry.isEmpty()) {
					this.#map.delete(location);
				}
			}
		}
	}

	export namespace Tokens {
		export class Entry {
			authorization: Array<Token>;

			constructor(authorization: Array<Token> = []) {
				this.authorization = authorization;
			}

			isEmpty(): boolean {
				return this.authorization.length === 0;
			}

			inherit(parent: Entry): void {
				this.authorization.push(...parent.authorization);
				this.normalize();
			}

			// Keep the latest equivalent proofs per resource and prune proofs covered by the receiving object.
			normalize(resource?: string): void {
				let resources = new Map<string, Array<Token>>();
				let authorization: Array<Token> = [];
				let distinct = new Map(
					this.authorization.map((token) => [token.toData(), token]),
				);
				for (let token of [...distinct.values()].sort(compare)) {
					let resource = token.body?.resource;
					if (resource === undefined) {
						authorization.push(token);
						continue;
					}
					let proofs = resources.get(resource) ?? [];
					if (proofs.some((existing) => existing.covers(token))) {
						continue;
					}
					proofs = proofs.filter((existing) => !token.covers(existing));
					proofs.push(token);
					resources.set(resource, proofs);
				}
				for (let proofs of resources.values()) {
					authorization.push(...proofs);
				}
				// Keep sync proofs so readers can wait for objects still being transferred.
				if (
					resource !== undefined &&
					authorization.some((token) => token.authorizesObjectSubtree(resource))
				) {
					authorization = authorization.filter(
						(token) =>
							token.body?.resource.startsWith("syn_") ||
							token.authorizesObjectSubtree(resource),
					);
				}
				authorization.sort((a, b) =>
					a.toData() < b.toData() ? -1 : a.toData() > b.toData() ? 1 : 0,
				);
				this.authorization = authorization;
			}
		}
		export type Data = Record<string, Array<Token.Data>>;

		export namespace Data {
			export let clone = (tokens: Tokens | Data | null | undefined): Data => {
				if (tokens instanceof Tokens) {
					return tokens.toData();
				}
				return Object.fromEntries(
					Object.entries(tokens ?? {}).map(([location, entry]) => [
						location,
						[...entry],
					]),
				);
			};

			export let isEmpty = (tokens: Tokens | Data): boolean =>
				tokens instanceof Tokens
					? tokens.isEmpty()
					: Object.values(tokens).every((entry) => entry.length === 0);
			export let local = (tokens: Data): Array<string> | null =>
				tokens.local ?? null;
			export let withLocal = (entry: Array<string> | null): Data => {
				let tokens =
					entry === null || entry.length === 0 ? {} : { local: [...entry] };
				normalize(tokens);
				return tokens;
			};
			export let inherit = (
				tokens: Data,
				parent: Tokens | Data,
				resource?: string,
			): void => {
				let parsed = Tokens.fromData(tokens);
				parsed.inherit(Tokens.fromData(parent), resource);
				replace(tokens, parsed.toData());
			};
			export let normalize = (tokens: Data, resource?: string): void => {
				let parsed = Tokens.fromData(tokens);
				parsed.normalize(resource);
				replace(tokens, parsed.toData());
			};
			let replace = (tokens: Data, data: Data): void => {
				for (let location of Object.keys(tokens)) {
					delete tokens[location];
				}
				Object.assign(tokens, data);
			};
		}
	}

	let compare = (a: Token, b: Token): number => {
		let x = a.body?.expiresAt ?? -1n;
		let y = b.body?.expiresAt ?? -1n;
		if (x !== y) {
			return x > y ? -1 : 1;
		}
		return a.toData() < b.toData() ? -1 : a.toData() > b.toData() ? 1 : 0;
	};

	// Decode the body, secret ID, and signature.
	let decode = (
		token: string,
	): { body: Body; secretId: bigint; signature: Array<number> } => {
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
		let secretId: bigint;
		[secretId, position] = readUvarint(bytes, position);
		if (position >= bytes.length) {
			throw new Error("missing the signature");
		}
		let body = { expiresAt: expiration, permissions, resource };
		let signature = bytes.slice(position);
		return { body, secretId, signature };
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

	// Encode the bytes, joining the symbols at the end so that the output is a flat string that keeps cached decoded tokens compact.
	let encodeBase32 = (bytes: Array<number>): string => {
		let output: Array<string> = [];
		let buffer = 0;
		let bits = 0;
		for (let byte of bytes) {
			buffer = ((buffer << 8) | byte) & 0xfff;
			bits += 8;
			while (bits >= 5) {
				bits -= 5;
				output.push(symbols[(buffer >> bits) & 31]!);
			}
		}
		if (bits > 0) {
			output.push(symbols[(buffer << (5 - bits)) & 31]!);
		}
		return output.join("");
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
		18: "rmt",
		19: "grt",
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
