import { test } from "node:test";
import assert from "node:assert/strict";
import { shouldDeliverHAEvent, type HAEvent } from "../src/ha-events.js";

function stateEvent(
  state: string,
  attributes: Record<string, unknown> = {},
): HAEvent {
  return {
    event_type: "state_changed",
    data: {
      entity_id: "climate.master_bedroom_ac",
      old_state: null,
      new_state: { state, attributes },
    },
    origin: "LOCAL",
    time_fired: "2026-10-10T00:00:00Z",
    context: {},
  };
}

test("deduplicates identical state samples", () => {
  const previous = new Map<string, string>();
  assert.equal(shouldDeliverHAEvent(stateEvent("off"), previous), true);
  assert.equal(shouldDeliverHAEvent(stateEvent("off"), previous), false);
});

test("keeps state and attribute changes", () => {
  const previous = new Map<string, string>();
  assert.equal(shouldDeliverHAEvent(stateEvent("off"), previous), true);
  assert.equal(shouldDeliverHAEvent(stateEvent("cool"), previous), true);
  assert.equal(
    shouldDeliverHAEvent(
      stateEvent("cool", { current_temperature: 22 }),
      previous,
    ),
    true,
  );
});

test("keeps non-state events and incomplete state events", () => {
  const previous = new Map<string, string>();
  const service: HAEvent = {
    ...stateEvent("off"),
    event_type: "call_service",
  };
  assert.equal(shouldDeliverHAEvent(service, previous), true);
  assert.equal(
    shouldDeliverHAEvent(
      {
        ...stateEvent("off"),
        data: { entity_id: "climate.master_bedroom_ac", new_state: null },
      },
      previous,
    ),
    true,
  );
});
