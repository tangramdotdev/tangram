import { assert } from "./assert.ts";

export type Module = {
	kind: string;
	referent: Referent;
};

type Referent = {
	item: Item;
	options?: Options;
};

type Item = string | Id | Reference;

type Reference = {
	graph: Id;
	node: number;
};

type Id = string;

type Options = {
	id?: string;
	name?: string;
	path?: string;
	tag?: string;
};

export namespace Module {
	export let toDataString = (value: Module): string => {
		let item = value.referent.item;
		let string = item.toString();
		let params = [];
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

	export let fromDataString = (data: string): Module => {
		let [item, params] = data.split("?");
		let kind: string | undefined;
		let options: Options = {};
		if (params !== undefined) {
			for (let param of params.split("&")) {
				let [key, value] = param.split("=");
				if (value === undefined) {
					throw new Error("missing value");
				}
				switch (key) {
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
						kind = decodeURIComponent(value);
						break;
					}
					case "extension": {
						break;
					}
					default: {
						throw new Error("invalid key");
					}
				}
			}
		}
		assert(kind !== undefined, "could not infer the module kind");
		let module = {
			kind,
			referent: {
				item: item!,
				options,
			},
		};
		return module;
	};
}
