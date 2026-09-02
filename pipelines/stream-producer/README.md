# Stream producer

This directory will contain the Wikimedia EventStreams client and optional Azure
relay. It validates recent-change events, removes forbidden identity and free-text
fields, and publishes only the allowlisted projection to Event Hubs.
