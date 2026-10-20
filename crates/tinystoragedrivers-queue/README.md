# tinystoragedrivers-queue

Queue implementations for `QueueStore`: a native in-memory engine by default,
a document-backed engine behind `document`, and RabbitMQ behind `rabbitmq`.

The queue manifest and its host configuration are intentionally outside this
crate. Hosts select an engine and declare queues through the `QueueStore`
contract.
