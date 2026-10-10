import { Fault, insist } from "./safety.js";
import {
  HA_WEBSOCKET_URL,
  type SocketFactory,
  type SocketLike,
} from "./ha-websocket.js";

/** Events accepted by the durable Home Assistant event bridge. */
export const HA_EVENT_TYPES = Object.freeze([
  "state_changed",
  "call_service",
  "automation_triggered",
  "device_automation_triggered",
] as const);
export type HAEventType = (typeof HA_EVENT_TYPES)[number];

export type HAEvent = {
  event_type: string;
  data: Record<string, unknown>;
  origin: string;
  time_fired: string;
  context: Record<string, unknown>;
};

export type HAEventSubscription = {
  eventTypes: readonly HAEventType[];
  entities?: readonly string[];
  reconnectMs?: number;
};

type EventMessage = {
  type?: unknown;
  id?: unknown;
  event?: unknown;
  success?: unknown;
};

/**
 * Subscribe to a bounded event set until the caller aborts.
 * HA does not replay events after disconnect, so callers must reconcile state
 * after this function returns or reconnects.
 */
export async function subscribeHAEvents(
  factory: SocketFactory,
  token: string,
  redact: (value: string) => string,
  subscription: HAEventSubscription,
  onEvent: (event: HAEvent) => Promise<void> | void,
  signal?: AbortSignal,
): Promise<void> {
  insist(token, "ha_unconfigured", 503);
  const eventTypes = [...new Set(subscription.eventTypes)];
  insist(
    eventTypes.length > 0 && eventTypes.length <= 8,
    "ha_event_scope",
    400,
  );
  for (const eventType of eventTypes)
    insist(HA_EVENT_TYPES.includes(eventType), "ha_event_scope", 400);
  const entities = new Set(subscription.entities ?? []);
  const reconnectMs = Math.max(100, subscription.reconnectMs ?? 1000);
  while (!signal?.aborted) {
    try {
      await subscribeOnce(
        factory,
        token,
        redact,
        eventTypes,
        entities,
        onEvent,
        signal,
      );
    } catch (error) {
      if (signal?.aborted) break;
      if (error instanceof Fault && error.code === "ha_event_scope")
        throw error;
    }
    if (!signal?.aborted)
      await new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, reconnectMs);
        signal?.addEventListener(
          "abort",
          () => {
            clearTimeout(timer);
            resolve();
          },
          { once: true },
        );
      });
  }
}

async function subscribeOnce(
  factory: SocketFactory,
  token: string,
  redact: (value: string) => string,
  eventTypes: readonly HAEventType[],
  entities: ReadonlySet<string>,
  onEvent: (event: HAEvent) => Promise<void> | void,
  signal?: AbortSignal,
): Promise<void> {
  const socket = factory(HA_WEBSOCKET_URL);
  let nextId = 1;
  let authenticated = false;
  let resolveClosed!: () => void;
  let rejectClosed!: (error: unknown) => void;
  const closed = new Promise<void>((resolve, reject) => {
    resolveClosed = resolve;
    rejectClosed = reject;
  });
  const fail = (error: unknown = new Fault(502, "ha_event_failed")) => {
    rejectClosed(error);
    try {
      socket.close();
    } catch {
      /* closed already */
    }
  };
  const onAbort = () => {
    resolveClosed();
    try {
      socket.close();
    } catch {
      /* closed already */
    }
  };
  signal?.addEventListener("abort", onAbort, { once: true });
  socket.onerror = () => fail();
  socket.onclose = () => {
    if (!signal?.aborted) fail();
    else resolveClosed();
  };
  socket.onmessage = (message) => {
    if (typeof message.data !== "string") return fail();
    let parsed: EventMessage;
    try {
      parsed = JSON.parse(redact(message.data)) as EventMessage;
    } catch {
      return fail();
    }
    if (parsed.type === "auth_required") {
      socket.send(JSON.stringify({ type: "auth", access_token: token }));
      return;
    }
    if (parsed.type === "auth_ok") {
      authenticated = true;
      for (const eventType of eventTypes) {
        const id = nextId++;
        socket.send(
          JSON.stringify({
            id,
            type: "subscribe_events",
            event_type: eventType,
          }),
        );
      }
      return;
    }
    if (!authenticated) return fail(new Fault(502, "ha_auth_failed"));
    if (parsed.type === "result" && parsed.success !== true)
      return fail(new Fault(502, "ha_event_failed"));
    if (
      parsed.type !== "event" ||
      !parsed.event ||
      typeof parsed.event !== "object"
    )
      return;
    const event = parsed.event as HAEvent;
    if (
      typeof event.event_type !== "string" ||
      !event.data ||
      typeof event.data !== "object"
    )
      return;
    const direct = (event.data as { entity_id?: unknown }).entity_id;
    const serviceData = (
      event.data as { service_data?: { entity_id?: unknown } }
    ).service_data;
    const entityId =
      typeof direct === "string" ? direct : serviceData?.entity_id;
    const matches = Array.isArray(entityId)
      ? entityId.some(
          (value) => typeof value === "string" && entities.has(value),
        )
      : typeof entityId === "string" && entities.has(entityId);
    if (entities.size && !matches) return;
    void Promise.resolve(onEvent(event)).catch(fail);
  };
  try {
    await closed;
  } finally {
    signal?.removeEventListener("abort", onAbort);
    socket.onmessage = socket.onerror = socket.onclose = null;
    try {
      socket.close();
    } catch {
      /* closed already */
    }
  }
}
