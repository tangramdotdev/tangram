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

	export type Item = string | tg.Object;

	export let toData = (value: tg.Module): tg.Module.Data => {
		return {
			kind: value.kind,
			referent: tg.Referent.toData(value.referent, (item) =>
				typeof item === "string" ? item : item.id,
			),
		};
	};

	export let fromData = (data: tg.Module.Data): tg.Module => {
		return {
			kind: data.kind,
			referent: tg.Referent.fromData(data.referent, (item) => {
				if (item.startsWith(".") || item.startsWith("/")) {
					return item;
				} else {
					return tg.Object.withId(item);
				}
			}),
		};
	};

	export let toDataString = (value: tg.Module): string => {
		let item = value.referent.item;
		let string = item.toString();
		let params = [];
		if (value.referent.options?.artifact !== undefined) {
			params.push(
				`artifact=${encodeURIComponent(value.referent.options.artifact)}`,
			);
		}
		if (value.referent.options?.id !== undefined) {
			params.push(`id=${encodeURIComponent(value.referent.options.id)}`);
		}
		if (value.referent.options?.name !== undefined) {
			params.push(`name=${encodeURIComponent(value.referent.options.name)}`);
		}
		if (value.referent.options?.path !== undefined) {
			params.push(`path=${encodeURIComponent(value.referent.options.path)}`);
		}
		if (value.referent.options?.tag !== undefined) {
			params.push(`tag=${encodeURIComponent(value.referent.options.tag)}`);
		}
		params.push(`kind=${encodeURIComponent(value.kind)}`);
		string += "?";
		string += params.join("&");
		return string;
	};

	export let fromDataString = (data: string): tg.Module => {
		let [itemString, params] = data.split("?");
		let kind: tg.Module.Kind | undefined;
		let item = tg.Object.withId(itemString!);
		let options: tg.Referent.Data.Options = {};
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
			return [value.referent.item];
		} else {
			return [];
		}
	};

	export type Data = {
		kind: Module.Kind;
		referent: tg.Referent.Data<tg.Object.Id>;
	};
}
