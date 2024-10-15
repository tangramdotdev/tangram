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

	export let components = (pathArg: string): Array<Component> => {
		let subComponents = pathArg.split("/");
		if ((subComponents.at(-1) as string).length === 0) {
			subComponents.pop();
		}
		return pathArg.startsWith("/")
			? [Component.Root, ...subComponents.slice(1)]
			: subComponents;
	};

	export let normalize = (pathArg: string): string => {
		let oldComponents = path.components(pathArg);
		let newComponents: Array<string> = [];

		for (let component of oldComponents) {
			// Skip any current dir components.
			if (component === Component.Current) {
				continue;
			}

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

	export let parent = (pathArg: string): string | undefined => {
		if (pathArg.length === 0) {
			throw new Error("expected a non-empty string");
		}
		let pathComponents = path.components(pathArg);
		if (pathComponents.length === 0) {
			return undefined;
		}
		return pathComponents.slice(0, -1).join("/");
	};

	export let isAbsolute = (pathArg: string): boolean => {
		if (pathArg.length === 0) {
			return false;
		}
		let component = path.components(pathArg)[0];
		return component === Component.Root;
	};

	export let isExternal = (pathArg: string): boolean => {
		if (pathArg.length === 0) {
			throw new Error("expected a non-empty string");
		}
		let component = path.components(pathArg)[0];
		return component === Component.Parent;
	};

	export let isInternal = (pathArg: string): boolean => {
		if (pathArg.length === 0) {
			throw new Error("expected a non-empty string");
		}
		let component = path.components(pathArg)[0] as Component;
		return Component.isNormal(component) || component === Component.Current;
	};

	export let join = (...paths: Array<string | undefined>): string => {
		let allComponents: Array<string> = [];
		for (let path_ of paths) {
			if (path_ === undefined) {
				continue;
			}
			if (isAbsolute(path_)) {
				allComponents = path.components(path_);
			} else {
				allComponents = allComponents.concat(path.components(path_));
			}
		}
		return path.fromComponents(allComponents);
	};

	export let fromComponents = (components: Array<Component>): string => {
		if (components.length === 0) {
			return "";
		} else if (components[0] === Component.Root) {
			return `/${components.slice(1).join("/")}`;
		} else {
			return components.join("/");
		}
	};
}
