//! `RabbitMQ` queue engine implementation.

mod envelope;
mod link;
mod names;
mod queues;

pub use names::MAX_DELAY_MS;
pub use queues::RabbitQueues;
