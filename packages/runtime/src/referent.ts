import * as tg from "./index.ts";

export type Referent<T> = {
	item: T;
	options: tg.Referent.Options;
};

export namespace Referent {
	export type Options = {
		id?: tg.Object.Id | undefined;
		name?: string | undefined;
		path?: string | undefined;
		process?: tg.Process.Id | undefined;
		tag?: tg.Tag | undefined;
	};

	export let toData = <T, U>(
		value: tg.Referent<T>,
		f: (item: T) => U,
	): tg.Referent.Data<U> => {
		let item = f(value.item);
		let options: tg.Referent.Data.Options = {};
		if (value.options?.id !== undefined) {
			options.id = value.options.id;
		}
		if (value.options?.name !== undefined) {
			options.name = value.options.name;
		}
		if (value.options?.path !== undefined) {
			options.path = value.options.path;
		}
		if (value.options?.process !== undefined) {
			options.process = value.options.process;
		}
		if (value.options?.tag !== undefined) {
			options.tag = value.options.tag;
		}
		return {
			item,
			options,
		};
	};

	export let fromData = <T, U>(
		data: tg.Referent.Data<T>,
		f: (item: T) => U,
	): tg.Referent<U> => {
		tg.assert(typeof data === "object");
		let item = f(data.item);
		let options = data.options ?? {};
		return {
			item,
			options,
		};
	};

	export let toDataString = <T, U extends string>(
		value: tg.Referent<T>,
		f: (item: T) => U,
	): string => {
		let item = f(value.item);
		let string = item.toString();
		let params = [];
		if (value.options?.id !== undefined) {
			params.push(`id=${encodeURIComponent(value.options.id)}`);
		}
		if (value.options?.name !== undefined) {
			params.push(`name=${encodeURIComponent(value.options.name)}`);
		}
		if (value.options?.path !== undefined) {
			params.push(`path=${encodeURIComponent(value.options.path)}`);
		}
		if (value.options?.tag !== undefined) {
			params.push(`tag=${encodeURIComponent(value.options.tag)}`);
		}
		if (params.length > 0) {
			string += "?";
			string += params.join("&");
		}
		return string;
	};

	export let fromDataString = <T extends string, U>(
		data: string,
		f: (item: T) => U,
	): tg.Referent<U> => {
		let [itemString, params] = data.split("?");
		let item = f(itemString! as T);
		let options: tg.Referent.Data.Options = {};
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
					default: {
						throw new Error("invalid key");
					}
				}
			}
		}
		let referent: tg.Referent<U> = {
			item,
			options,
		};
		return referent;
	};

	export type Data<T> =
		| string
		| {
				item: T;
				options?: tg.Referent.Data.Options;
		  };

	export namespace Data {
		export type Options = {
			id?: tg.Object.Id;
			name?: string;
			path?: string;
			process?: tg.Process.Id;
			tag?: tg.Tag;
		};
	}
}
