import * as tg from "./index.ts";
import { Resolve } from "./resolve.ts";

export class Module {
	[Resolve.atomic]: null;
	kind: Module.Kind;
	referent: tg.Referent<tg.Module.Source>;

	constructor(arg: {
		kind: Module.Kind;
		referent: tg.Referent<tg.Module.Source>;
	}) {
		this[Resolve.atomic] = null;
		this.kind = arg.kind;
		this.referent = arg.referent;
	}
}

export namespace Module {
	export type Kind =
		| "js"
		| "ts"
		| "dts"
		| "object"
		| "artifact"
		| "blob"
		| "directory"
		| "file"
		| "symlink"
		| "graph"
		| "command";

	export type Source = string | tg.Graph.Edge<tg.Object>;

	export namespace Source {
		export let toDataString = (value: tg.Module.Source): string => {
			if (typeof value === "string") {
				if (value.startsWith(".") || value.startsWith("/")) {
					return value;
				} else {
					return `./${value}`;
				}
			} else {
				return tg.Graph.Edge.toDataString(value, (object) => object.id);
			}
		};
	}

	export let toData = (value: tg.Module): tg.Module.Data => {
		return {
			kind: value.kind,
			referent: tg.Referent.toData(value.referent, (source) =>
				typeof source === "string"
					? source
					: tg.Graph.Edge.toDataString(source, (object) => object.id),
			),
		};
	};

	export let fromData = (data: tg.Module.Data): tg.Module => {
		return new tg.Module({
			kind: data.kind,
			referent: tg.Referent.fromData(data.referent, (source) => {
				if (
					typeof source === "string" &&
					(source.startsWith(".") || source.startsWith("/"))
				) {
					return source;
				} else {
					return tg.Graph.Edge.fromData(source, tg.Object.withId);
				}
			}),
		});
	};

	export let toDataString = (value: tg.Module): string => {
		let string = tg.Module.Source.toDataString(value.referent.node);
		let params = [];
		if (
			value.referent.options?.artifact !== undefined &&
			value.referent.options.artifact !== null
		) {
			params.push(
				`artifact=${encodeURIComponent(value.referent.options.artifact)}`,
			);
		}
		if (
			value.referent.options?.id !== undefined &&
			value.referent.options.id !== null
		) {
			params.push(`id=${encodeURIComponent(value.referent.options.id)}`);
		}
		if (
			value.referent.options?.location !== undefined &&
			value.referent.options.location !== null
		) {
			let location = tg.Location.toDataString(value.referent.options.location);
			params.push(`location=${encodeURIComponent(location)}`);
		}
		if (
			value.referent.options?.name !== undefined &&
			value.referent.options.name !== null
		) {
			params.push(`name=${encodeURIComponent(value.referent.options.name)}`);
		}
		if (
			value.referent.options?.path !== undefined &&
			value.referent.options.path !== null
		) {
			params.push(`path=${encodeURIComponent(value.referent.options.path)}`);
		}
		if (
			value.referent.options?.tag !== undefined &&
			value.referent.options.tag !== null
		) {
			params.push(`tag=${encodeURIComponent(value.referent.options.tag)}`);
		}
		for (let [location, token] of Object.entries(
			value.referent.options?.tokens ?? {},
		)) {
			params.push(
				`tokens[${encodeURIComponent(location)}]=${encodeURIComponent(token)}`,
			);
		}
		params.push(`kind=${encodeURIComponent(value.kind)}`);
		string += "?";
		string += params.join("&");
		return string;
	};

	export let fromDataString = (data: string): tg.Module => {
		let [nodeString, params] = data.split("?");
		tg.assert(nodeString !== undefined);
		let kind: tg.Module.Kind | undefined;
		let source: tg.Module.Source;
		if (
			typeof nodeString === "string" &&
			(nodeString.startsWith(".") || nodeString.startsWith("/"))
		) {
			source = nodeString;
		} else {
			source = tg.Graph.Edge.fromDataString(nodeString, tg.Object.withId);
		}
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
					case "kind": {
						kind = decodeURIComponent(value) as Kind;
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
		tg.assert(kind !== undefined);
		let module = new tg.Module({
			kind,
			referent: {
				node: source,
				options,
			},
		});
		return module;
	};

	export let children = (value: Module): Array<tg.Object> => {
		let children =
			typeof value.referent.node !== "string"
				? tg.Graph.Edge.children(value.referent.node)
				: [];
		for (let child of children) {
			tg.Object.inheritLocation(
				child,
				value.referent.options?.location ?? null,
			);
			tg.Object.inheritTokens(child, value.referent.options?.tokens ?? {});
		}
		return children;
	};

	export let withoutToken = (value: tg.Module): tg.Module => {
		return new tg.Module({
			kind: value.kind,
			referent: tg.Referent.withoutToken(value.referent),
		});
	};

	export type Data = {
		kind: Module.Kind;
		referent: tg.Referent.Data<tg.Graph.Data.Edge<tg.Object.Id>>;
	};

	export namespace Data {
		export let children = (data: tg.Module.Data): Array<tg.Object.Id> => {
			let source =
				typeof data.referent === "string" ? data.referent : data.referent.node;
			if (
				typeof source === "string" &&
				(source.startsWith(".") || source.startsWith("/"))
			) {
				return [];
			}
			return tg.Graph.Data.Edge.children(source);
		};

		export let withoutTokens = (data: tg.Module.Data): tg.Module.Data => {
			if (typeof data.referent === "string") {
				let referent = tg.Referent.fromDataString(
					data.referent,
					(source) => source,
				);
				return {
					...data,
					referent: tg.Referent.toDataString(
						tg.Referent.withoutRuntime(referent),
						(source) => source,
					),
				};
			}
			let referent = tg.Referent.fromData(data.referent, (source) => source);
			return {
				...data,
				referent: tg.Referent.toData(
					tg.Referent.withoutRuntime(referent),
					(source) => source,
				),
			};
		};
	}

	export type Location = {
		module: tg.Module;
		range: tg.Range;
	};

	export namespace Location {
		export type Data = {
			module: tg.Module.Data;
			range: tg.Range;
		};

		export let toData = (
			value: tg.Module.Location,
		): tg.Module.Location.Data => {
			return {
				module: tg.Module.toData(value.module),
				range: value.range,
			};
		};

		export let fromData = (
			data: tg.Module.Location.Data,
		): tg.Module.Location => {
			return {
				module: tg.Module.fromData(data.module),
				range: data.range,
			};
		};

		export let children = (value: tg.Module.Location): Array<tg.Object> => {
			return tg.Module.children(value.module);
		};

		export namespace Data {
			export let children = (
				data: tg.Module.Location.Data,
			): Array<tg.Object.Id> => {
				return tg.Module.Data.children(data.module);
			};

			export let withoutTokens = (
				data: tg.Module.Location.Data,
			): tg.Module.Location.Data => {
				return {
					...data,
					module: tg.Module.Data.withoutTokens(data.module),
				};
			};
		}
	}
}
