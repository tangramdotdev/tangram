export namespace path {
	export type Component =
		| Component.Normal
		| Component.Current
		| Component.Parent
		| Component.Root;

	export namespace Component {
		export type Normal = string;

		export type Current = ".";

		export let Current = ".";

		export type Parent = "..";

		export let Parent = "..";

		export type Root = "/";

		export let Root = "/";

		export let isNormal = (component: Component): component is Normal => {
			return (
				typeof component === "string" &&
				component !== Current &&
				component !== Parent &&
				component !== Root
			);
		};
	}

	export let components = (path: string): Array<Component> => {
		let subComponents = path.split("/");
		if ((subComponents.at(-1) as string).length === 0) {
			subComponents.pop();
		}
		return path.startsWith("/")
			? [Component.Root, ...subComponents.slice(1)]
			: subComponents;
	};

	export let normalize = (path: string): string => {
		let oldComponents = components(path);
		let newComponents: Array<string> = [];

		for (let component of oldComponents) {
			let last = newComponents.at(-1);
			if (
				component === Component.Parent &&
				last !== undefined &&
				Component.isNormal(last)
			) {
				newComponents.pop();
			} else {
				newComponents.push(component);
			}
		}

		let newpath = newComponents.join("/");
		if (isAbsolute(newpath)) {
			return newpath.slice(1);
		} else {
			return newpath;
		}
	};

	export let parent = (path: string): string | undefined => {
		if (path.length === 0) {
			throw new Error("expected a non-empty string");
		}
		let pathComponents = components(path);
		if (pathComponents.length === 0) {
			return undefined;
		}
		return pathComponents.slice(0, -1).join("/");
	};

	export let isAbsolute = (path: string): boolean => {
		if (path.length === 0) {
			throw new Error("expected a non-empty string");
		}
		let component = components(path)[0];
		return component === Component.Root;
	};

	export let isExternal = (path: string): boolean => {
		if (path.length === 0) {
			throw new Error("expected a non-empty string");
		}
		let component = components(path)[0];
		return component === Component.Parent;
	};

	export let isInternal = (path: string): boolean => {
		if (path.length === 0) {
			throw new Error("expected a non-empty string");
		}
		let component = components(path)[0] as Component;
		return (
			Component.isNormal(component) || component === Component.Current
		);
	};

	export let join = (...paths: Array<string | undefined>): string => {
		let allComponents: Array<string> = [];
		for (let path of paths) {
			if (path === undefined) {
				continue;
			}
			if (isAbsolute(path)) {
				allComponents = components(path);
			} else {
				allComponents = allComponents.concat(components(path));
			}
		}
		return path.fromComponents(allComponents);
	};

	export let fromComponents = (components: Array<Component>): string => {
		if (components.length === 0) {
			return "";
		} else if (components[0] === Component.Root) {
			return "/" + components.slice(1).join("/");
		} else {
			return components.join("/");
		}
	};
}
