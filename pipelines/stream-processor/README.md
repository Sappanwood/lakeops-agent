# Stream processor

This directory will contain the Event Hubs consumer, event-time watermark and
checkpoint handling, recent-change validation, dead-letter behavior, and
immutable Parquet micro-batch writer. Checkpoints advance only after the matching
micro-batch and manifest are durable.
