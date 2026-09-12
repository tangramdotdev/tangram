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

		export let covers = (token: Token, other: Token): boolean => {
			if (token === other) {
				return true;
			}
			let a = parse(token);
			let b = parse(other);
			return (
				a !== null &&
				b !== null &&
				a.metadata.algorithm === b.metadata.algorithm &&
				a.metadata.key === b.metadata.key &&
				a.body.resource === b.body.resource &&
				a.body.expires_at >= b.body.expires_at &&
				b.body.permissions.every((needed) =>
					a.body.permissions.some((granted) => implies(granted, needed)),
				)
			);
		};

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
}
