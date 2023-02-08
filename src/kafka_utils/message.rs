use protobuf::{MessageDyn};
use super::{key::Key, topic::Topic};

pub struct Message {
    pub topic: Topic,
    pub key: Key,
    pub protobuf_message: Box<dyn MessageDyn>,
}

impl Message {
    pub fn new(topic: Topic, key: Key, protobuf_message: impl MessageDyn) -> Self {
        Message {
            topic,
            key,
            protobuf_message: Box::new(protobuf_message),
        }
    }
}

#[cfg(test)]
mod tests {
    use semver::Version;
    use super::*;
    use super::super::proto_test::{utils, evm::Block};
    use super::super::topic::*;
    use super::super::key::*;

    #[test]
    fn create_new_message() {
        let eth_block = utils::build_block();
        let version = Version::new(1, 2, 3);

        let topic = Topic{
            env: Env::Prod,
            message_type: MessageType::BF,
            author: "nakji".to_string(),
            connector_name: "ethereum".to_string(),
            version,
            event_name: "chain_Block".to_string(),
        };

        let key = Key { namespace: "ethereum".to_string(), subject: "Transaction".to_string() };

        let message = Message::new(topic.clone(), key.clone(), eth_block.clone());
        let expected = Message {
            protobuf_message: Box::new(eth_block),
            topic,
            key,
        };

        assert_eq!(message.topic, expected.topic);
        assert_eq!(message.key, expected.key);



        assert_eq!(message.protobuf_message.downcast_box::<Block>().unwrap(), expected.protobuf_message.downcast_box::<Block>().unwrap());
    }
}