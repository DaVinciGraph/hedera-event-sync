import { AsyncLocalStorage } from "node:async_hooks";
import { LifecycleReentrancyError } from "../errors";

export type UserCallbackContext = {
	kind: "hook" | "handler" | "normalizer" | "resolver" | "until";
	name: string;
	queryId: number | null;
};

type ActiveUserCallback = UserCallbackContext & { active: boolean; owner?: object };

const userCallbackStorage = new AsyncLocalStorage<readonly ActiveUserCallback[]>();
const callbackOwners = new WeakMap<object, object>();

export function associateUserCallbackOwner<T extends object>(value: T, owner?: object): T {
	if (owner) callbackOwners.set(value, owner);
	return value;
}

export function userCallbackOwnerOf(value: object): object | undefined {
	return callbackOwners.get(value);
}

/** Marks package-invoked user code so lifecycle controls cannot wait on themselves. */
export async function runInUserCallback<T>(context: UserCallbackContext, callback: () => T | Promise<T>, owner?: object): Promise<T> {
	const activeContext: ActiveUserCallback = { ...context, active: true, owner };
	// Nested standalone processors and other synchronizers must not hide the
	// outer callback whose completion a lifecycle operation would still await.
	const ancestry = [...(userCallbackStorage.getStore()?.filter((ancestor) => ancestor.active) ?? []), activeContext];
	try {
		return await userCallbackStorage.run(ancestry, callback);
	} finally {
		// Descendant async contexts share these entries. Clear only this callback:
		// a settled inner callback can still have an active outer callback, while
		// completed callbacks must no longer retain their synchronizer owners.
		activeContext.active = false;
		activeContext.owner = undefined;
	}
}

export function assertOutsideUserCallback(operation: string, owner?: object): void {
	const ancestry = userCallbackStorage.getStore();
	if (!ancestry) return;
	for (let index = ancestry.length - 1; index >= 0; index--) {
		const context = ancestry[index];
		if (context.active && (!owner || context.owner === owner)) {
			throw new LifecycleReentrancyError(operation, context);
		}
	}
}
