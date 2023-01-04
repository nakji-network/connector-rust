use crate::kafka_utils::producer::{Producer};

pub struct Connector {
    producer: Producer,
    proto_registry_host: String,
}

impl Connector {
    pub fn new() -> Self {
        todo!()
    }
}