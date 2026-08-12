import * as tg from "./index.ts";

export type Reference = tg.Reference.String;

export namespace Reference {
	export type String = string;

	export type Object<T> = {
		node: T;
		options?: tg.Reference.Options;
	};

	export type Options = {
		artifact?: tg.Artifact.Id | null;
		get?: string | null;
		id?: tg.Object.Id | null;
		location?: tg.Location.Arg | null;
		name?: string | null;
		path?: string | null;
		source?: string | null;
		tag?: tg.Tag | null;
		tokens?: tg.Authorization.Tokens | null;
	};

	export let toData = <T, U>(
		value: tg.Reference.Object<T>,
		f: (node: T) => U,
	): tg.Reference.Data<U> => {
		let node = f(value.node);
		let options: tg.Reference.Data.Options = {};
		if (
			value.options?.artifact !== undefined &&
			value.options.artifact !== null
		) {
			options.artifact = value.options.artifact;
		}
		if (value.options?.get !== undefined && value.options.get !== null) {
			options.get = value.options.get;
		}
		if (value.options?.id !== undefined && value.options.id !== null) {
			options.id = value.options.id;
		}
		if (
			value.options?.location !== undefined &&
			value.options.location !== null
		) {
			options.location = tg.Location.Arg.toDataString(value.options.location);
		}
		if (value.options?.name !== undefined && value.options.name !== null) {
			options.name = value.options.name;
		}
		if (value.options?.path !== undefined && value.options.path !== null) {
			options.path = value.options.path;
		}
		if (value.options?.source !== undefined && value.options.source !== null) {
			options.source = value.options.source;
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
		data: tg.Reference.Data<T>,
		f: (node: T) => U,
	): tg.Reference.Object<U> => {
		tg.assert(typeof data === "object");
		let node = f(data.node);
		let options: tg.Reference.Options = {};
		if (
			data.options?.artifact !== undefined &&
			data.options.artifact !== null
		) {
			options.artifact = data.options.artifact;
		}
		if (data.options?.get !== undefined && data.options.get !== null) {
			options.get = data.options.get;
		}
		if (data.options?.id !== undefined && data.options.id !== null) {
			options.id = data.options.id;
		}
		if (
			data.options?.location !== undefined &&
			data.options.location !== null
		) {
			options.location = tg.Location.Arg.fromDataString(data.options.location);
		}
		if (data.options?.name !== undefined && data.options.name !== null) {
			options.name = data.options.name;
		}
		if (data.options?.path !== undefined && data.options.path !== null) {
			options.path = data.options.path;
		}
		if (data.options?.source !== undefined && data.options.source !== null) {
			options.source = data.options.source;
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
		value: tg.Reference.Object<T>,
		f: (node: T) => U,
	): tg.Reference.String => {
		let node = f(value.node);
		let string = node.toString();
		let params = [];
		if (
			value.options?.artifact !== undefined &&
			value.options.artifact !== null
		) {
			params.push(`artifact=${encodeURIComponent(value.options.artifact)}`);
		}
		if (value.options?.get !== undefined && value.options.get !== null) {
			params.push(`get=${encodeURIComponent(value.options.get)}`);
		}
		if (value.options?.id !== undefined && value.options.id !== null) {
			params.push(`id=${encodeURIComponent(value.options.id)}`);
		}
		if (
			value.options?.location !== undefined &&
			value.options.location !== null
		) {
			let location = tg.Location.Arg.toDataString(value.options.location);
			params.push(`location=${encodeURIComponent(location)}`);
		}
		if (value.options?.name !== undefined && value.options.name !== null) {
			params.push(`name=${encodeURIComponent(value.options.name)}`);
		}
		if (value.options?.path !== undefined && value.options.path !== null) {
			params.push(`path=${encodeURIComponent(value.options.path)}`);
		}
		if (value.options?.source !== undefined && value.options.source !== null) {
			params.push(`source=${encodeURIComponent(value.options.source)}`);
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
		data: tg.Reference.String,
		f: (node: T) => U,
	): tg.Reference.Object<U> => {
		let [nodeString, params] = data.split("?");
		let node = f(nodeString! as T);
		let options: tg.Reference.Options = {};
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
					case "get": {
						options.get = decodeURIComponent(value);
						break;
					}
					case "id": {
						options.id = decodeURIComponent(value);
						break;
					}
					case "location": {
						options.location = tg.Location.Arg.fromDataString(
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
					case "source": {
						options.source = decodeURIComponent(value);
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
		let reference: tg.Reference.Object<U> = {
			node,
			options,
		};
		return reference;
	};

	export type Data<T> =
		| string
		| {
				node: T;
				options?: tg.Reference.Data.Options;
		  };

	export namespace Data {
		export let withoutTokens = <T>(
			data: tg.Reference.Data<T>,
		): tg.Reference.Data<T> => {
			if (typeof data === "string") {
				let reference = tg.Reference.fromDataString(data, (node) => node);
				let options = { ...reference.options };
				delete options.tokens;
				return tg.Reference.toDataString(
					{ ...reference, options },
					(node) => node,
				);
			}
			let output = { ...data };
			if (output.options !== undefined) {
				output.options = { ...output.options };
				delete output.options.tokens;
			}
			return output;
		};

		export type Options = {
			artifact?: tg.Artifact.Id | null;
			get?: string | null;
			id?: tg.Object.Id | null;
			location?: string | null;
			name?: string | null;
			path?: string | null;
			source?: string | null;
			tag?: tg.Tag | null;
			tokens?: tg.Authorization.Tokens | null;
		};
	}
}
