import type { Process, ProcessStep, ProcessTry, ProcessWithoutPayload, ReadCycleAudit } from "../types/domain";
import type { RuntimeQueryConfig } from "../types/config";

const PROCESS_PAYLOAD_KEYS = new Set<PropertyKey>(["data", "rawData"]);

function cloneValue<T>(value: T, seen = new WeakMap<object, unknown>(), omittedKeys?: ReadonlySet<PropertyKey>): T {
	if (value === null || typeof value !== "object") return value;
	const source = value as object;
	const existing = seen.get(source);
	if (existing !== undefined) return existing as T;

	try {
		if (value instanceof Date) return new Date(value.getTime()) as T;

		const isArray = Array.isArray(value);
		const prototype = isArray ? Array.prototype : Object.getPrototypeOf(value);
		if (!isArray && prototype !== Object.prototype && prototype !== null) return value;

		const copy: object = isArray ? [] : Object.create(prototype);
		seen.set(source, copy);
		const descriptors = Object.getOwnPropertyDescriptors(value) as Record<PropertyKey, PropertyDescriptor | undefined>;
		let arrayLength: PropertyDescriptor | undefined;

		for (const key of Reflect.ownKeys(descriptors)) {
			if (omittedKeys?.has(key)) continue;
			const descriptor = descriptors[key];
			if (!descriptor) continue;
			if (isArray && key === "length") {
				arrayLength = descriptor;
				continue;
			}

			// Reading through the source would execute accessors. Copy accessors as
			// descriptors and recursively clone only concrete data properties.
			const clonedDescriptor = { ...descriptor };
			if ("value" in clonedDescriptor) clonedDescriptor.value = cloneValue(clonedDescriptor.value, seen);
			Object.defineProperty(copy, key, clonedDescriptor);
		}

		// Defining array length last allows sparse/trailing slots to be preserved
		// even when the source length is non-writable.
		if (arrayLength) Object.defineProperty(copy, "length", arrayLength);
		return copy as T;
	} catch {
		// Some proxies throw from prototype, descriptor, or own-key traps. Such
		// values are opaque application objects: retaining their identity is safer
		// than executing accessors or letting an observer snapshot break a runtime.
		seen.set(source, source);
		return value;
	}
}

export function snapshotValue<T>(value: T): T {
	return cloneValue(value);
}

export function snapshotQuery(query: RuntimeQueryConfig): RuntimeQueryConfig {
	return cloneValue(query);
}

export function snapshotProcess(process: Process): Process;
export function snapshotProcess(process: Process, includePayload: true): Process;
export function snapshotProcess(process: Process, includePayload: false): ProcessWithoutPayload;
export function snapshotProcess(process: Process, includePayload: boolean): Process | ProcessWithoutPayload;
export function snapshotProcess(process: Process, includePayload = true): Process | ProcessWithoutPayload {
	// Exclude potentially hostile payloads before traversal rather than cloning
	// them and deleting them afterward.
	return cloneValue(process, new WeakMap<object, unknown>(), includePayload ? undefined : PROCESS_PAYLOAD_KEYS);
}

export function snapshotReadCycle(audit: ReadCycleAudit): ReadCycleAudit {
	return cloneValue(audit);
}

export function snapshotProcessTry(processTry: ProcessTry): ProcessTry {
	return cloneValue(processTry);
}

export function snapshotProcessStep(step: ProcessStep): ProcessStep {
	return cloneValue(step);
}
