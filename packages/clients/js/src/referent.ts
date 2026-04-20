import * as tg from "./index.ts";

export type Referent<T> = {
	item: T;
	options?: tg.Referent.Options | undefined;
};

export namespace Referent {
	export type Options = {
		artifact?: tg.Artifact.Id | undefined;
		id?: tg.Object.Id | undefined;
		location?: tg.Location.Arg | undefined;
		name?: string | undefined;
		path?: string | undefined;
		tag?: tg.Tag | undefined;
	};

	export let toData = <T, U>(
		value: tg.Referent<T>,
		f: (item: T) => U,
	): tg.Referent.Data<U> => {
		let item = f(value.item);
		let options: tg.Referent.Data.Options = {};
		if (value.options?.artifact !== undefined) {
			options.artifact = value.options.artifact;
		}
		if (value.options?.id !== undefined) {
			options.id = value.options.id;
		}
		if (value.options?.location !== undefined) {
			options.location = tg.Location.Arg.toDataString(value.options.location);
		}
		if (value.options?.name !== undefined) {
			options.name = value.options.name;
		}
		if (value.options?.path !== undefined) {
			options.path = value.options.path;
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
		let options: tg.Referent.Options = {};
		if (data.options?.artifact !== undefined) {
			options.artifact = data.options.artifact;
		}
		if (data.options?.id !== undefined) {
			options.id = data.options.id;
		}
		if (data.options?.location !== undefined) {
			options.location = tg.Location.Arg.fromDataString(data.options.location);
		}
		if (data.options?.name !== undefined) {
			options.name = data.options.name;
		}
		if (data.options?.path !== undefined) {
			options.path = data.options.path;
		}
		if (data.options?.tag !== undefined) {
			options.tag = data.options.tag;
		}
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
		if (value.options?.artifact !== undefined) {
			params.push(`artifact=${encodeURIComponent(value.options.artifact)}`);
		}
		if (value.options?.id !== undefined) {
			params.push(`id=${encodeURIComponent(value.options.id)}`);
		}
		if (value.options?.location !== undefined) {
			let location = tg.Location.Arg.toDataString(value.options.location);
			params.push(`location=${encodeURIComponent(location)}`);
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
			artifact?: tg.Artifact.Id;
			id?: tg.Object.Id;
			location?: string;
			name?: string;
			path?: string;
			tag?: tg.Tag;
		};
	}
}
