use std::env;
use std::fs::File;

use serde_yaml::Value;

use crate::kafka_utils::Env;

pub struct Config {
    pub kafka_url: String,
    pub kafka_env: Env,
    pub proto_registry_host: String,
}

const CONFIG_KEY_KAFKA: &str = "kafka";
const CONFIG_KEY_KAFKA_URL: &str = "url";
const CONFIG_KEY_KAFKA_ENV: &str = "env";
const CONFIG_KEY_PROTO_REGISTRY: &str = "protoregistry";
const CONFIG_KEY_PROTO_REGISTRY_HOST: &str = "host";

impl Config {
    pub fn init() -> Self {
        let yaml = Config::read_from_file();

        let kafka_url = yaml[CONFIG_KEY_KAFKA][CONFIG_KEY_KAFKA_URL]
            .as_str()
            .expect("kafka url should be a string")
            .to_string();
        let kafka_env: Env = yaml[CONFIG_KEY_KAFKA][CONFIG_KEY_KAFKA_ENV]
            .as_str()
            .expect("kafka env should be a string")
            .parse()
            .expect("wrong env value");
        let proto_registry_host = yaml[CONFIG_KEY_PROTO_REGISTRY][CONFIG_KEY_PROTO_REGISTRY_HOST]
            .as_str()
            .expect("proto registry host should be a string")
            .to_string();

        Config {
            kafka_url,
            kafka_env,
            proto_registry_host,
        }
    }

    fn read_from_file() -> Value {
        let config_file_name = "config.yaml";
        let config_path = env::var("CONFIGPATH").unwrap_or_else(|_| "$HOME/.config".to_string());
        let default_config_paths = vec![
            "./".to_string(),
            config_path.clone(),
            config_path + "/nakji/",
            "/etc/nakji/".to_string(),
        ];

        let files: Vec<_> = default_config_paths.into_iter().map(|p| p + config_file_name).filter_map(|f: String| File::open(f).ok()).collect();

        let file = files.get(0).expect("failed to find the config.yaml");
        let yaml: Value = serde_yaml::from_reader(file).expect("failed to deserialize the config.yaml");

        yaml
    }
}
