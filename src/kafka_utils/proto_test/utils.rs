use super::chain;

pub fn build_block() -> chain::Block {
    chain::Block {
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

pub fn build_transaction() -> chain::Transaction {
    chain::Transaction {
        ts: Default::default(),
        from: vec![],
        hash: "".to_string(),
        size: 0.0,
        account_nonce: 0,
        price: 0,
        gas_limit: 0,
        recipient: vec![],
        amount: 0,
        payload: vec![],
        v: 0,
        r: 0,
        s: 0,
        special_fields: Default::default(),
    }
}