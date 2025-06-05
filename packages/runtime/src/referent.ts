import * as tg from "./index.ts";

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

	export let toData = <T, U = T>(
		value: tg.Referent<T>,
		f?: (item: T) => U,
	): tg.Referent.Data<U> => {
		// @ts-ignore
		let string = (f !== undefined ? f(value.item) : value.item).toString();
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
	};

	export let fromData = <T, U = T>(
		data: tg.Referent.Data<T>,
		f?: (item: T) => U,
	): tg.Referent<U> => {
		if (typeof data === "string") {
			let [itemString, params] = data.split("?");
			tg.assert(itemString !== undefined);
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
				item: f !== undefined ? f(data.item) : (data.item as unknown as U),
			};
		}
	};
}
