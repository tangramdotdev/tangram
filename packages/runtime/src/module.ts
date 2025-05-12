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

	export type Data = {
		kind: Module.Kind;
		referent: tg.Referent<tg.Object.Id>;
	};

	export let toData = (value: Module): Data => {
		return {
			kind: value.kind,
			referent: {
				...value.referent,
				item:
					typeof value.referent.item === "string"
						? value.referent.item
						: value.referent.item.id,
			},
		};
	};

	export let fromData = (data: Data): Module => {
		return {
			kind: data.kind,
			referent: {
				...data.referent,
				item: tg.Object.withId(data.referent.item),
			},
		};
	};

	export let children = (value: Module): Array<tg.Object> => {
		if (typeof value.referent.item !== "string") {
			return [value.referent.item];
		} else {
			return [];
		}
	};
}
