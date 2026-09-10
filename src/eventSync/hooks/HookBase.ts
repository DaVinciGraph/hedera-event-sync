import type { SyncHooks } from "./SyncHooks";

/**
 * No-op hook implementation for consumers that prefer class-based callbacks.
 * The merged interface makes every optional hook visible as a function-valued
 * property, preserving strict parameter variance when a subclass is upcast.
 */
export interface HookBase extends SyncHooks {}
export class HookBase {}
