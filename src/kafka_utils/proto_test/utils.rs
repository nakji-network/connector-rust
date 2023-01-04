use super::ethereum;

pub fn build_block() -> ethereum::Block {
    ethereum::Block {
        ts: Default::default(),
        hash: "".to_string(),
        difficulty: 0,
        number: 0,
        gas_limit: 0,
        gas_used: 0,
        nonce: 0,
        special_fields: Default::default(),
    }
}