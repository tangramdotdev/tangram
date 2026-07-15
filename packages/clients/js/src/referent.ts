import * as tg from "./index.ts";

export type Referent<T> = {
	item: T;
	options?: tg.Referent.Options;
};

export namespace Referent {
	export type Options = {
		artifact?: tg.Artifact.Id | null;
		id?: tg.Object.Id | null;
		location?: tg.Location.Arg | null;
		name?: string | null;
		path?: string | null;
		tag?: tg.Tag | null;
		token?: tg.Grant.Token | null;
	};

	export let toData = <T, U>(
		value: tg.Referent<T>,
		f: (item: T) => U,
	): tg.Referent.Data<U> => {
		let item = f(value.item);
		let options: tg.Referent.Data.Options = {};
		if (
			value.options?.artifact !== undefined &&
			value.options.artifact !== null
		) {
			options.artifact = value.options.artifact;
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
		if (value.options?.tag !== undefined && value.options.tag !== null) {
			options.tag = value.options.tag;
		}
		if (value.options?.token !== undefined && value.options.token !== null) {
			options.token = value.options.token;
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
		if (
			data.options?.artifact !== undefined &&
			data.options.artifact !== null
		) {
			options.artifact = data.options.artifact;
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
		if (data.options?.tag !== undefined && data.options.tag !== null) {
			options.tag = data.options.tag;
		}
		if (data.options?.token !== undefined && data.options.token !== null) {
			options.token = data.options.token;
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
		if (
			value.options?.artifact !== undefined &&
			value.options.artifact !== null
		) {
			params.push(`artifact=${encodeURIComponent(value.options.artifact)}`);
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
		if (value.options?.tag !== undefined && value.options.tag !== null) {
			params.push(`tag=${encodeURIComponent(value.options.tag)}`);
		}
		if (value.options?.token !== undefined && value.options.token !== null) {
			params.push(`token=${encodeURIComponent(value.options.token)}`);
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
					case "token": {
						options.token = decodeURIComponent(value);
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

	export let withoutToken = <T>(value: tg.Referent<T>): tg.Referent<T> => {
		let referent: tg.Referent<T> = {
			item: value.item,
		};
		if (value.options !== undefined) {
			referent.options = { ...value.options };
			delete referent.options.token;
		}
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
			artifact?: tg.Artifact.Id | null;
			id?: tg.Object.Id | null;
			location?: string | null;
			name?: string | null;
			path?: string | null;
			tag?: tg.Tag | null;
			token?: tg.Grant.Token | null;
		};
	}
}
