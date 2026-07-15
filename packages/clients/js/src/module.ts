import * as tg from "./index.ts";

export type Module = {
	kind: Module.Kind;
	referent: tg.Referent<tg.Module.Item>;
};

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

	export type Item = string | tg.Graph.Edge<tg.Object>;

	export namespace Item {
		export let toDataString = (value: tg.Module.Item): string => {
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
			referent: tg.Referent.toData(value.referent, (item) =>
				typeof item === "string"
					? item
					: tg.Graph.Edge.toDataString(item, (object) => object.id),
			),
		};
	};

	export let fromData = (data: tg.Module.Data): tg.Module => {
		return {
			kind: data.kind,
			referent: tg.Referent.fromData(data.referent, (item) => {
				if (
					typeof item === "string" &&
					(item.startsWith(".") || item.startsWith("/"))
				) {
					return item;
				} else {
					return tg.Graph.Edge.fromData(item, tg.Object.withId);
				}
			}),
		};
	};

	export let toDataString = (value: tg.Module): string => {
		let string = tg.Module.Item.toDataString(value.referent.item);
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
			let location = tg.Location.Arg.toDataString(
				value.referent.options.location,
			);
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
		if (
			value.referent.options?.token !== undefined &&
			value.referent.options.token !== null
		) {
			params.push(`token=${encodeURIComponent(value.referent.options.token)}`);
		}
		params.push(`kind=${encodeURIComponent(value.kind)}`);
		string += "?";
		string += params.join("&");
		return string;
	};

	export let fromDataString = (data: string): tg.Module => {
		let [itemString, params] = data.split("?");
		tg.assert(itemString !== undefined);
		let kind: tg.Module.Kind | undefined;
		let item: tg.Module.Item;
		if (
			typeof itemString === "string" &&
			(itemString.startsWith(".") || itemString.startsWith("/"))
		) {
			item = itemString;
		} else {
			item = tg.Graph.Edge.fromDataString(itemString, tg.Object.withId);
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
					case "tag": {
						options.tag = decodeURIComponent(value);
						break;
					}
					case "token": {
						options.token = decodeURIComponent(value);
						break;
					}
					case "kind": {
						kind = decodeURIComponent(value) as Kind;
						break;
					}
					default: {
						throw new Error("invalid key");
					}
				}
			}
		}
		tg.assert(kind !== undefined);
		let module = {
			kind,
			referent: {
				item,
				options,
			},
		};
		return module;
	};

	export let children = (value: Module): Array<tg.Object> => {
		if (typeof value.referent.item !== "string") {
			return tg.Graph.Edge.children(value.referent.item);
		} else {
			return [];
		}
	};

	export let withoutToken = (value: tg.Module): tg.Module => {
		return {
			kind: value.kind,
			referent: tg.Referent.withoutToken(value.referent),
		};
	};

	export type Data = {
		kind: Module.Kind;
		referent: tg.Referent.Data<tg.Graph.Data.Edge<tg.Object.Id>>;
	};

	export namespace Data {
		export let children = (data: tg.Module.Data): Array<tg.Object.Id> => {
			let item =
				typeof data.referent === "string" ? data.referent : data.referent.item;
			if (
				typeof item === "string" &&
				(item.startsWith(".") || item.startsWith("/"))
			) {
				return [];
			}
			return tg.Graph.Data.Edge.children(item);
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
		}
	}
}
