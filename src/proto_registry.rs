use std::{env, fs};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use log::info;
use protobuf::MessageDyn;
use serde::Serialize;
use walkdir::{DirEntry, WalkDir};

use crate::kafka_utils::{MessageType, TOPIC_CONTEXT_SEPARATOR};

const DESCRIPTOR_OUTPUT_DIR: &str = "DESCRIPTOR_OUTPUT_DIR";

#[derive(Serialize, Debug, PartialEq, Clone)]
struct TopicProtoMsg {
    #[serde(rename(serialize = "msg_type"))]
    message_type: MessageType,
    #[serde(rename(serialize = "topic"))]
    topic_name: String,
    #[serde(rename(serialize = "proto_msg"))]
    proto_message_name: String,
    descriptor: Vec<u8>,
}


pub async fn register_dynamic_topics(host: &str, topic_types: HashMap<String, Box<dyn MessageDyn>>, message_type: MessageType) {
    let mut topic_proto_messages = build_topic_proto_messages(topic_types, message_type);
    generate_descriptor_files(&mut topic_proto_messages);
    let bytes = serde_json::to_vec(&topic_proto_messages).expect("failed to serialize topic_proto_messages to bytes");

    let client = reqwest::Client::new();

    let res = client.post(host.to_string() + "/v1/register")
        .body(bytes)
        .send()
        .await
        .unwrap_or_else(|err| panic!("request to protoregistry failed: {err:?}"));
    info!("response: {:#?}", res);
}

fn build_topic_proto_messages(topic_types: HashMap<String, Box<dyn MessageDyn>>, message_type: MessageType) -> Vec<TopicProtoMsg> {
    let mut topic_proto_messages: Vec<TopicProtoMsg> = Vec::new();

    for (topic, message) in topic_types {
        let message_descriptor = message.descriptor_dyn();

        let topic_proto_message = TopicProtoMsg {
            message_type: message_type.clone(),
            topic_name: topic,
            proto_message_name: message_descriptor.full_name().to_string(),
            descriptor: vec![],
        };
        topic_proto_messages.push(topic_proto_message);
    }

    topic_proto_messages
}

fn generate_descriptor_files(topic_proto_messages: &mut Vec<TopicProtoMsg>) {
    let cwd = env::current_dir().expect("failed to get the current working dir");

    for tpm in topic_proto_messages {
        let full_package: Vec<_> = tpm.proto_message_name.split(TOPIC_CONTEXT_SEPARATOR).collect();
        let package = full_package.get(full_package.len() - 2).unwrap_or_else(|| panic!("invalid full package: {:?}", full_package));
        let file_name = package.to_string() + ".proto";
        let path = get_proto_file_path(&cwd, &file_name).unwrap_or_else(|| panic!("failed to get the proto file path, cwd: {:?}, file name: {}", cwd, file_name));
        let desc_file_name = generate_descriptor_file(path.path());
        let descriptor = fs::read(desc_file_name.clone()).unwrap_or_else(|_| panic!("failed to read descriptor file: {:?}", desc_file_name));
        tpm.descriptor = descriptor;
    }
}

fn get_proto_file_path(base_dir: &PathBuf, file_name: &String) -> Option<DirEntry> {
    WalkDir::new(base_dir)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| !e.file_type().is_dir())
        .find(|e| String::from(e.file_name().to_string_lossy()) == *file_name)
}

fn generate_descriptor_file(path: &Path) -> String {
    let file = path.file_name().unwrap_or_else(|| panic!("failed to get the file name, path: {:?}", path)).to_str().expect("failed to convert file to str");
    let dir = path.parent().unwrap_or_else(|| panic!("failed to get the path parent, path: {:?}", path)).to_str().expect("failed to convert dir to str").trim_end_matches("/");

    let desc_file_name = match env::var(DESCRIPTOR_OUTPUT_DIR) {
        Ok(output_dir) => format!("{}/{}.desc", output_dir, file),
        Err(_) => format!("{}/{}.desc", dir, file),
    };
    if Path::exists(desc_file_name.as_ref()) {
        info!("descriptor file: {} already exists, skip", desc_file_name);
        return desc_file_name;
    }

    Command::new("protoc")
        .arg("--include_imports")
        .arg("--descriptor_set_out=".to_string() + &desc_file_name)
        .arg("-I=".to_string() + dir)
        .arg(file)
        .output()
        .expect("failed to execute process");

    desc_file_name
}


#[cfg(test)]
mod tests {
    use crate::kafka_utils::proto_test::utils;

    use super::*;

    #[test]
    fn test_build_topic_proto_messages() {
        let eth_block = utils::build_block();

        let mut topic_types: HashMap<String, Box<dyn MessageDyn>> = HashMap::new();
        topic_types.insert("nakji.protoregistry.0_0_0.chain_Block".to_string(), Box::new(eth_block.clone()));

        let topic_proto_messages = build_topic_proto_messages(topic_types, MessageType::SYS);

        let mut expected: Vec<TopicProtoMsg> = Vec::new();

        let tpm = TopicProtoMsg {
            message_type: MessageType::SYS,
            topic_name: "nakji.protoregistry.0_0_0.chain_Block".to_string(),
            proto_message_name: "nakji.evm.Block".to_string(),
            descriptor: vec![],
        };
        expected.push(tpm);

        assert_eq!(topic_proto_messages, expected)
    }

    #[test]
    fn test_generate_descriptor_file() {
        let tpm1 = TopicProtoMsg {
            message_type: MessageType::CMD,
            topic_name: "nakji.ethereum.0_0_0.chain_Block".to_string(),
            proto_message_name: "nakji.evm.Block".to_string(),
            descriptor: vec![],
        };

        let tpm2 = TopicProtoMsg {
            message_type: MessageType::CMD,
            topic_name: "nakji.ethereum.0_0_0.chain_Transaction".to_string(),
            proto_message_name: "nakji.evm.Transaction".to_string(),
            descriptor: vec![],
        };

        generate_descriptor_files(&mut vec![tpm1, tpm2]);

        let cwd = env::current_dir().expect("failed to get the current working dir");
        let mut desc = cwd.join("src").join("kafka_utils").join("proto_test");
        desc.push("evm.proto.desc");

        let bytes = fs::read(&desc).expect("failed to read .desc");
        let expected: Vec<u8> = vec![10, 255, 1, 10, 31, 103, 111, 111, 103, 108, 101, 47, 112, 114, 111, 116, 111, 98, 117, 102, 47, 116, 105, 109, 101, 115, 116, 97, 109, 112, 46, 112, 114, 111, 116, 111, 18, 15, 103, 111, 111, 103, 108, 101, 46, 112, 114, 111, 116, 111, 98, 117, 102, 34, 59, 10, 9, 84, 105, 109, 101, 115, 116, 97, 109, 112, 18, 24, 10, 7, 115, 101, 99, 111, 110, 100, 115, 24, 1, 32, 1, 40, 3, 82, 7, 115, 101, 99, 111, 110, 100, 115, 18, 20, 10, 5, 110, 97, 110, 111, 115, 24, 2, 32, 1, 40, 5, 82, 5, 110, 97, 110, 111, 115, 66, 133, 1, 10, 19, 99, 111, 109, 46, 103, 111, 111, 103, 108, 101, 46, 112, 114, 111, 116, 111, 98, 117, 102, 66, 14, 84, 105, 109, 101, 115, 116, 97, 109, 112, 80, 114, 111, 116, 111, 80, 1, 90, 50, 103, 111, 111, 103, 108, 101, 46, 103, 111, 108, 97, 110, 103, 46, 111, 114, 103, 47, 112, 114, 111, 116, 111, 98, 117, 102, 47, 116, 121, 112, 101, 115, 47, 107, 110, 111, 119, 110, 47, 116, 105, 109, 101, 115, 116, 97, 109, 112, 112, 98, 248, 1, 1, 162, 2, 3, 71, 80, 66, 170, 2, 30, 71, 111, 111, 103, 108, 101, 46, 80, 114, 111, 116, 111, 98, 117, 102, 46, 87, 101, 108, 108, 75, 110, 111, 119, 110, 84, 121, 112, 101, 115, 98, 6, 112, 114, 111, 116, 111, 51, 10, 217, 4, 10, 9, 101, 118, 109, 46, 112, 114, 111, 116, 111, 18, 9, 110, 97, 107, 106, 105, 46, 101, 118, 109, 26, 31, 103, 111, 111, 103, 108, 101, 47, 112, 114, 111, 116, 111, 98, 117, 102, 47, 116, 105, 109, 101, 115, 116, 97, 109, 112, 46, 112, 114, 111, 116, 111, 34, 199, 2, 10, 11, 84, 114, 97, 110, 115, 97, 99, 116, 105, 111, 110, 18, 42, 10, 2, 116, 115, 24, 1, 32, 1, 40, 11, 50, 26, 46, 103, 111, 111, 103, 108, 101, 46, 112, 114, 111, 116, 111, 98, 117, 102, 46, 84, 105, 109, 101, 115, 116, 97, 109, 112, 82, 2, 116, 115, 18, 18, 10, 4, 102, 114, 111, 109, 24, 2, 32, 1, 40, 12, 82, 4, 102, 114, 111, 109, 18, 18, 10, 4, 104, 97, 115, 104, 24, 3, 32, 1, 40, 9, 82, 4, 104, 97, 115, 104, 18, 18, 10, 4, 115, 105, 122, 101, 24, 4, 32, 1, 40, 1, 82, 4, 115, 105, 122, 101, 18, 35, 10, 13, 97, 99, 99, 111, 117, 110, 116, 95, 110, 111, 110, 99, 101, 24, 5, 32, 1, 40, 4, 82, 12, 97, 99, 99, 111, 117, 110, 116, 78, 111, 110, 99, 101, 18, 20, 10, 5, 112, 114, 105, 99, 101, 24, 6, 32, 1, 40, 4, 82, 5, 112, 114, 105, 99, 101, 18, 27, 10, 9, 103, 97, 115, 95, 108, 105, 109, 105, 116, 24, 7, 32, 1, 40, 4, 82, 8, 103, 97, 115, 76, 105, 109, 105, 116, 18, 28, 10, 9, 114, 101, 99, 105, 112, 105, 101, 110, 116, 24, 8, 32, 1, 40, 12, 82, 9, 114, 101, 99, 105, 112, 105, 101, 110, 116, 18, 22, 10, 6, 97, 109, 111, 117, 110, 116, 24, 9, 32, 1, 40, 4, 82, 6, 97, 109, 111, 117, 110, 116, 18, 24, 10, 7, 112, 97, 121, 108, 111, 97, 100, 24, 10, 32, 1, 40, 12, 82, 7, 112, 97, 121, 108, 111, 97, 100, 18, 12, 10, 1, 118, 24, 11, 32, 1, 40, 4, 82, 1, 118, 18, 12, 10, 1, 114, 24, 12, 32, 1, 40, 4, 82, 1, 114, 18, 12, 10, 1, 115, 24, 13, 32, 1, 40, 4, 82, 1, 115, 34, 205, 1, 10, 5, 66, 108, 111, 99, 107, 18, 42, 10, 2, 116, 115, 24, 1, 32, 1, 40, 11, 50, 26, 46, 103, 111, 111, 103, 108, 101, 46, 112, 114, 111, 116, 111, 98, 117, 102, 46, 84, 105, 109, 101, 115, 116, 97, 109, 112, 82, 2, 116, 115, 18, 18, 10, 4, 104, 97, 115, 104, 24, 2, 32, 1, 40, 9, 82, 4, 104, 97, 115, 104, 18, 30, 10, 10, 100, 105, 102, 102, 105, 99, 117, 108, 116, 121, 24, 3, 32, 1, 40, 4, 82, 10, 100, 105, 102, 102, 105, 99, 117, 108, 116, 121, 18, 22, 10, 6, 110, 117, 109, 98, 101, 114, 24, 4, 32, 1, 40, 4, 82, 6, 110, 117, 109, 98, 101, 114, 18, 27, 10, 9, 103, 97, 115, 95, 108, 105, 109, 105, 116, 24, 5, 32, 1, 40, 4, 82, 8, 103, 97, 115, 76, 105, 109, 105, 116, 18, 25, 10, 8, 103, 97, 115, 95, 117, 115, 101, 100, 24, 6, 32, 1, 40, 4, 82, 7, 103, 97, 115, 85, 115, 101, 100, 18, 20, 10, 5, 110, 111, 110, 99, 101, 24, 7, 32, 1, 40, 4, 82, 5, 110, 111, 110, 99, 101, 98, 6, 112, 114, 111, 116, 111, 51];

        assert_eq!(bytes, expected);

        // clean up .desc
        fs::remove_file(&desc).expect("failed to remove .desc");
    }

    #[test]
    #[should_panic]
    fn test_generate_descriptor_file_with_wrong_proto_message_name() {
        let tpm = TopicProtoMsg {
            message_type: MessageType::CMD,
            topic_name: "nakji.ethereum.0_0_0.chain_Block".to_string(),
            proto_message_name: "random.Block".to_string(),
            descriptor: vec![],
        };

        generate_descriptor_files(&mut vec![tpm]);
    }
}