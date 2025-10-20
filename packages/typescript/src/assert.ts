export let assert: (condition: unknown, message?: string) => asserts condition =
	(condition, message) => {
		if (!condition) {
			throw new Error(message ?? "failed assertion");
		}
	};

export let unimplemented = (message?: string): never => {
	throw new Error(message ?? "unimplemented");
};

export let unreachable = (message?: string): never => {
	throw new Error(message ?? "unreachable");
};

export let todo = (message?: string): never => {
	throw new Error(message ?? "todo");
};
