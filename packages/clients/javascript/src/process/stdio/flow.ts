import type { Stdio } from "../../config.ts";

export function capacity(config: Stdio): number {
	return config.limits.messages * 2 + 4;
}
