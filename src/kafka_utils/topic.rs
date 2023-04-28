use std::fmt;
use std::str::FromStr;

use protobuf::MessageDyn;
use semver::Version;
use serde::Serialize;

pub const TOPIC_CONTEXT_SEPARATOR: &str = ".";
pub const TOPIC_CONTRACT_SEPARATOR: &str = "_";
pub const TOPIC_AGGREGATE_SEPARATOR: &str = "-";
pub const TOPIC_WILDCARD_SUFFIX: &str = "-*";
pub const TOPIC_NUM_SEGMENTS: i32 = 4;

#[derive(Debug, PartialEq, Clone)]
pub struct Topic {
    pub env: Env,
    pub message_type: MessageType,
    pub author: String,
    pub connector_name: String,
    pub version: Version,
    pub event_name: String,
}

impl Topic {
    pub fn new(env: Env, message_type: MessageType, author: String, connector_name: String,
               version: Version, event_name: String) -> Self {
        Self {
            env,
            message_type,
            author,
            connector_name,
            version,
            event_name,
        }
    }

    pub fn to_schema(&self) -> String {
        let version = self.version.to_string().replace(TOPIC_CONTEXT_SEPARATOR, TOPIC_CONTRACT_SEPARATOR);

        let vec: Vec<&str> = vec![
            self.author.as_str(),
            &self.connector_name,
            &version,
            &self.event_name,
        ];

        vec.join(TOPIC_CONTEXT_SEPARATOR)
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = vec![self.env.as_str(), self.message_type.as_str(), &self.to_schema()].join(TOPIC_CONTEXT_SEPARATOR);
        write!(f, "{s}")
    }
}


#[derive(Debug, PartialEq, Clone)]
pub enum Env {
    Test,
    Dev,
    Staging,
    Prod,
}

impl Env {
    pub fn as_str(&self) -> &str {
        match self {
            Env::Test => "test",
            Env::Dev => "dev",
            Env::Staging => "staging",
            Env::Prod => "prod",
        }
    }
}

impl FromStr for Env {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "test" => Ok(Env::Test),
            "dev" => Ok(Env::Dev),
            "staging" => Ok(Env::Staging),
            "prod" => Ok(Env::Prod),
            _ => Err(format!("'{}' is not a valid value for topic::Env", s)),
        }
    }
}

#[derive(Debug, PartialEq, Clone, Serialize)]
#[serde(rename_all(serialize = "lowercase"))]
pub enum MessageType {
    FCT,
    BF,
    CDC,
    CMD,
    SYS,
}

impl MessageType {
    fn as_str(&self) -> &str {
        match self {
            MessageType::FCT => "fct",
            MessageType::BF => "bf",
            MessageType::CDC => "cdc",
            MessageType::CMD => "cmd",
            MessageType::SYS => "sys",
        }
    }
}

pub fn get_event_name(protobuf_message: Box<dyn MessageDyn>) -> String {
    let message_descriptor = protobuf_message.descriptor_dyn();
    let full_name = message_descriptor.full_name().to_string();
    let name_slice: Vec<_> = full_name.split(TOPIC_CONTEXT_SEPARATOR).collect();
    let event_name = &name_slice[name_slice.len() - 2..].join(TOPIC_CONTRACT_SEPARATOR);
    event_name.clone()
}

#[cfg(test)]
mod tests {
    use crate::kafka_utils::proto_test::utils;

    use super::*;

    #[test]
    fn get_event_name_from_protobuf() {
        let eth_block = utils::build_block();
        let event_name = get_event_name(Box::new(eth_block));
        assert_eq!(event_name, "evm_Block")
    }

    #[test]
    fn create_new_topic() {
        let version = Version::new(1, 2, 3);

        let topic = Topic::new(
            Env::Test,
            MessageType::FCT,
            "nakji".to_string(),
            "ethereum".to_string(),
            version.clone(),
            "evm_Block".to_string(),
        );

        let expected = Topic {
            env: Env::Test,
            message_type: MessageType::FCT,
            author: "nakji".to_string(),
            connector_name: "ethereum".to_string(),
            version,
            event_name: "evm_Block".to_string(),
        };

        assert_eq!(topic, expected);
    }

    #[test]
    fn topic_to_schema() {
        let version = Version::new(0, 1, 0);

        let topic = Topic::new(
            Env::Test,
            MessageType::FCT,
            "nakji".to_string(),
            "ethereum".to_string(),
            version,
            "evm_Block".to_string(),
        );

        let schema = topic.to_schema();

        assert_eq!(schema, "nakji.ethereum.0_1_0.evm_Block");
    }

    #[test]
    fn topic_to_str() {
        let version = Version::new(3, 2, 1);

        let topic = Topic::new(
            Env::Dev,
            MessageType::FCT,
            "nakji".to_string(),
            "ethereum".to_string(),
            version,
            "chain_Block".to_string(),
        );

        let schema = topic.to_string();

        assert_eq!(schema, "dev.fct.nakji.ethereum.3_2_1.chain_Block");
    }
}