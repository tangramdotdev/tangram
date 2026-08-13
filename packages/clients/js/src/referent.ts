import * as tg from "./index.ts";

export type Referent<T> = {
	node: T;
	options?: tg.Referent.Options;
};

export namespace Referent {
	export type Options = {
		artifact?: tg.Artifact.Id | null;
		id?: tg.Object.Id | null;
		location?: tg.Location | null;
		name?: string | null;
		path?: string | null;
		tag?: tg.Tag | null;
		tokens?: tg.Authorization.Tokens | null;
	};

	export let withNodeAndToken = <T>(
		node: T,
		token: tg.Authorization.Token | null,
	): tg.Referent<T> => {
		return withNodeAndTokens(node, tg.Authorization.Tokens.withLocal(token));
	};

	export let withNodeAndTokens = <T>(
		node: T,
		tokens: tg.Authorization.Tokens,
	): tg.Referent<T> => {
		let referent: tg.Referent<T> = { node };
		if (Object.keys(tokens).length > 0) {
			referent.options = { tokens };
		}
		return referent;
	};

	export let toData = <T, U>(
		value: tg.Referent<T>,
		f: (node: T) => U,
	): tg.Referent.Data<U> => {
		let node = f(value.node);
		let options: tg.Referent.Data.Options = {};
		if (
			value.options?.artifact !== undefined &&
			value.options.artifact !== null
		) {
			options.artifact = value.options.artifact;
		}
		if (value.options?.id !== undefined && value.options.id !== null) {
			options.id = value.options.id;
		}
		if (
			value.options?.location !== undefined &&
			value.options.location !== null
		) {
			options.location = tg.Location.toDataString(value.options.location);
		}
		if (value.options?.name !== undefined && value.options.name !== null) {
			options.name = value.options.name;
		}
		if (value.options?.path !== undefined && value.options.path !== null) {
			options.path = value.options.path;
		}
		if (value.options?.tag !== undefined && value.options.tag !== null) {
			options.tag = value.options.tag;
		}
		if (value.options?.tokens !== undefined && value.options.tokens !== null) {
			options.tokens = value.options.tokens;
		}
		return {
			node,
			options,
		};
	};

	export let fromData = <T, U>(
		data: tg.Referent.Data<T>,
		f: (node: T) => U,
	): tg.Referent<U> => {
		tg.assert(typeof data === "object");
		let node = f(data.node);
		let options: tg.Referent.Options = {};
		if (
			data.options?.artifact !== undefined &&
			data.options.artifact !== null
		) {
			options.artifact = data.options.artifact;
		}
		if (data.options?.id !== undefined && data.options.id !== null) {
			options.id = data.options.id;
		}
		if (
			data.options?.location !== undefined &&
			data.options.location !== null
		) {
			options.location = tg.Location.fromDataString(data.options.location);
		}
		if (data.options?.name !== undefined && data.options.name !== null) {
			options.name = data.options.name;
		}
		if (data.options?.path !== undefined && data.options.path !== null) {
			options.path = data.options.path;
		}
		if (data.options?.tag !== undefined && data.options.tag !== null) {
			options.tag = data.options.tag;
		}
		if (data.options?.tokens !== undefined && data.options.tokens !== null) {
			options.tokens = data.options.tokens;
		}
		return {
			node,
			options,
		};
	};

	export let toDataString = <T, U extends string>(
		value: tg.Referent<T>,
		f: (node: T) => U,
	): string => {
		let node = f(value.node);
		let string = node.toString();
		let params = [];
		if (
			value.options?.artifact !== undefined &&
			value.options.artifact !== null
		) {
			params.push(`artifact=${encodeURIComponent(value.options.artifact)}`);
		}
		if (value.options?.id !== undefined && value.options.id !== null) {
			params.push(`id=${encodeURIComponent(value.options.id)}`);
		}
		if (
			value.options?.location !== undefined &&
			value.options.location !== null
		) {
			let location = tg.Location.toDataString(value.options.location);
			params.push(`location=${encodeURIComponent(location)}`);
		}
		if (value.options?.name !== undefined && value.options.name !== null) {
			params.push(`name=${encodeURIComponent(value.options.name)}`);
		}
		if (value.options?.path !== undefined && value.options.path !== null) {
			params.push(`path=${encodeURIComponent(value.options.path)}`);
		}
		if (value.options?.tag !== undefined && value.options.tag !== null) {
			params.push(`tag=${encodeURIComponent(value.options.tag)}`);
		}
		for (let [location, token] of Object.entries(value.options?.tokens ?? {})) {
			params.push(
				`tokens[${encodeURIComponent(location)}]=${encodeURIComponent(token)}`,
			);
		}
		if (params.length > 0) {
			string += "?";
			string += params.join("&");
		}
		return string;
	};

	export let fromDataString = <T extends string, U>(
		data: string,
		f: (node: T) => U,
	): tg.Referent<U> => {
		let [nodeString, params] = data.split("?");
		let node = f(nodeString! as T);
		let options: tg.Referent.Options = {};
		if (params !== undefined) {
			for (let param of params.split("&")) {
				let [key, value] = param.split("=");
				if (value === undefined) {
					throw new Error("missing value");
				}
				switch (key) {
					case "artifact": {
						options.artifact = decodeURIComponent(value);
						break;
					}
					case "id": {
						options.id = decodeURIComponent(value);
						break;
					}
					case "location": {
						options.location = tg.Location.fromDataString(
							decodeURIComponent(value),
						);
						break;
					}
					case "name": {
						options.name = decodeURIComponent(value);
						break;
					}
					case "path": {
						options.path = decodeURIComponent(value);
						break;
					}
					case "tag": {
						options.tag = decodeURIComponent(value);
						break;
					}
					default: {
						let match = key?.match(/^tokens\[(.*)\]$/);
						if (match === null || match === undefined) {
							throw new Error("invalid key");
						}
						options.tokens ??= {};
						options.tokens[decodeURIComponent(match[1]!)] =
							decodeURIComponent(value);
					}
				}
			}
		}
		let referent: tg.Referent<U> = {
			node,
			options,
		};
		return referent;
	};

	export let withoutToken = <T>(value: tg.Referent<T>): tg.Referent<T> => {
		let referent: tg.Referent<T> = {
			node: value.node,
		};
		if (value.options !== undefined) {
			referent.options = { ...value.options };
			delete referent.options.tokens;
		}
		return referent;
	};

	export let withoutRuntime = <T>(value: tg.Referent<T>): tg.Referent<T> => {
		let referent = withoutToken(value);
		if (referent.options !== undefined) {
			delete referent.options.location;
		}
		return referent;
	};

	export type Data<T> =
		| string
		| {
				node: T;
				options?: tg.Referent.Data.Options;
		  };

	export namespace Data {
		export type Options = {
			artifact?: tg.Artifact.Id | null;
			id?: tg.Object.Id | null;
			location?: string | null;
			name?: string | null;
			path?: string | null;
			tag?: tg.Tag | null;
			tokens?: tg.Authorization.Tokens | null;
		};
	}
}
