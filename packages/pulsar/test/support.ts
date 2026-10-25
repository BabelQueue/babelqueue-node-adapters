import { PulsarConsumer } from "../src/index.js";
import type { BabelHandlers, PulsarConsumerClient, PulsarConsumerOptions } from "../src/index.js";

/**
 * A consumer on a `Shared` subscription: the nack retry is bounded there, so no retry producer is needed.
 * (An undeclared subscription type counts as Exclusive and is rejected without one.) `options` can override.
 */
export function sharedConsumer(
  client: PulsarConsumerClient,
  handlers: BabelHandlers,
  options: PulsarConsumerOptions = {},
): PulsarConsumer {
  return new PulsarConsumer(client, handlers, { subscriptionType: "Shared", ...options });
}
