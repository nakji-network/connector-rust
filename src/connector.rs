use std::collections::HashMap;

use log::debug;
use protobuf::MessageDyn;

use crate::proto_registry;
use crate::config::Config;
use crate::kafka_utils::{Env, MessageType, Producer, Topic, topic};
use crate::manifest::Manifest;

pub struct Connector {
    pub producer: Producer,
    pub config: Config,
    pub manifest: Manifest,
}

impl Connector {
    pub fn new() -> Self {
        let config = Config::init();
        let manifest = Manifest::init();
        let id = Connector::id(&manifest, &config);
        let producer = Producer::new(&config.kafka_url, &id);

        let sub_config = config.sub_config(id.as_str());
        
        Connector {
            producer,
            config: sub_config,
            manifest,
        }
    }

    fn id(manifest: &Manifest, config: &Config) -> String {
        format!("{}-{}-{}-{:?}", manifest.author, manifest.name, manifest.version, config.kafka_env)
    }

    pub async fn register_protos(&self, message_type: MessageType, protobuf_messages: Vec<Box<dyn MessageDyn>>) {
        if self.config.kafka_env == Env::Dev {
            debug!("protoregistry is disabled in dev mode, set kafka.env to other values (e.g., test, staging) to enable it");
            return;
        }

        let topic_types = self.build_topic_types(message_type.clone(), protobuf_messages);

        proto_registry::register_dynamic_topics(&self.config.proto_registry_host, topic_types, message_type).await;
    }

    fn build_topic_types(&self, message_type: MessageType, protobuf_messages: Vec<Box<dyn MessageDyn>>) -> HashMap<String, Box<dyn MessageDyn>> {
        let mut topic_types: HashMap<String, Box<dyn MessageDyn>> = HashMap::new();

        for message in protobuf_messages {
            let topic = self.create_topic_for_dynamic_message(message_type.clone(), message.clone());
            topic_types.insert(topic.to_schema(), message);
        }

        topic_types
    }

    pub fn create_topic_for_dynamic_message(&self, message_type: MessageType, message: Box<dyn MessageDyn>) -> Topic {
        let event_name = topic::get_event_name(message);

        Topic::new(
            self.config.kafka_env.clone(),
            message_type,
            self.manifest.author.clone(),
            self.manifest.name.clone(),
            self.manifest.version.clone(),
            event_name,
        )
    }
}