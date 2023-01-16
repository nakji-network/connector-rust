use std::fs::File;

use semver::Version;
use serde_yaml::Value;

pub struct Manifest {
    pub name: String,
    pub author: String,
    pub version: Version,
}

const MANIFEST_KEY_NAME: &str = "name";
const MANIFEST_KEY_AUTHOR: &str = "author";
const MANIFEST_KEY_VERSION: &str = "version";

impl Manifest {
    pub fn init() -> Self {
        let yaml = Manifest::read_from_file();

        let name = yaml[MANIFEST_KEY_NAME]
            .as_str()
            .expect("name should be a string")
            .to_string();
        let author = yaml[MANIFEST_KEY_AUTHOR]
            .as_str()
            .expect("author should be a string")
            .to_string();
        let version: Version = yaml[MANIFEST_KEY_VERSION]
            .as_str()
            .expect("version should be a string")
            .parse()
            .expect("wrong semantic versioning format");

        Manifest {
            name,
            author,
            version,
        }
    }

    fn read_from_file() -> Value {
        let manifest_file_name = "manifest.yaml";

        let file = File::open(manifest_file_name).expect("failed to find manifest.yaml");

        let yaml: Value = serde_yaml::from_reader(file).expect("failed to deserialize the config.yaml");

        yaml
    }
}