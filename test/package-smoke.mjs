import assert from "node:assert/strict";
import { HederaEventSync, HederaRestClient, HandlerRegistry, LifecycleBusyError } from "@davincigraph/hedera-event-sync";
import { define } from "@davincigraph/hedera-event-sync/types/config";

assert.equal(typeof HederaEventSync, "function");
assert.equal(typeof HandlerRegistry, "function");
assert.equal(typeof HederaRestClient, "function");
assert.equal(typeof LifecycleBusyError, "function");
assert.equal(typeof define, "function");
