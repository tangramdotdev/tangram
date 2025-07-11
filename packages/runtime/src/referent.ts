import type * as tg from "./index.ts";

export type Referent<T> = {
	item: T;
	path?: string | undefined;
	tag?: tg.Tag | undefined;
};

export namespace Referent {
	export type Data<T> =
		| string
		| {
				item: T;
				path?: string | undefined;
				tag?: tg.Tag | undefined;
		  };

	export let toData = <T, U>(
		value: tg.Referent<T>,
		f: (item: T) => U,
	): tg.Referent.Data<U> => {
		let item = f(value.item);
		if (typeof item === "string" || typeof item === "number") {
			let string = item.toString();
			if (value.path !== undefined || value.tag !== undefined) {
				string += "?";
			}
			if (value.path !== undefined) {
				string += `path=${encodeURIComponent(value.path)}`;
			}
			if (value.tag !== undefined) {
				if (value.path !== undefined) {
					string += "&";
				}
				string += `tag=${encodeURIComponent(value.tag)}`;
			}
			return string;
		} else {
			return {
				item,
				path: value.path,
				tag: value.tag,
			};
		}
	};

	export let fromData = <T, U>(
		data: tg.Referent.Data<T>,
		f: (item: T) => U,
	): tg.Referent<U> => {
		if (typeof data === "string") {
			let [itemString, params] = data.split("?");
			let item = f !== undefined ? f(itemString as T) : (itemString as U);
			let referent: tg.Referent<U> = { item };
			if (params !== undefined) {
				for (let param of params.split("&")) {
					let [key, value] = param.split("=");
					if (value === undefined) {
						throw new Error("missing value");
					}
					switch (key) {
						case "path": {
							referent.path = decodeURIComponent(value);
							break;
						}
						case "tag": {
							referent.tag = decodeURIComponent(value);
							break;
						}
						default: {
							throw new Error("invalid key");
						}
					}
				}
			}
			return referent;
		} else {
			return {
				...data,
				item: f(data.item),
			};
		}
	};
}
