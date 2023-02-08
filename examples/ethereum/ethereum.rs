use std::time::{Duration, SystemTime};

use ethers::prelude::{Block, H256, Middleware, Provider, StreamExt, Ws};
use eyre::Result;
use protobuf::well_known_types::timestamp::Timestamp;
use tokio::task;

use nakji_connector::connector::Connector;
use nakji_connector::kafka_utils::{Message, MessageType, topic, Topic};
use nakji_connector::kafka_utils::key::Key;

use crate::chain::Block as ProtoBlock;
use crate::chain::chain::Transaction as ProtoTransaction;

mod chain;

// change it to your own rpc url
const WSS_URL: &str = "your rpc url";

#[tokio::main]
async fn main() -> Result<()> {
    let block = ProtoBlock::new();

    let transaction = ProtoTransaction::new();
    let init_connector = Connector::new();
    let mut exec_connector = Connector::new();

    let event_name = topic::get_event_name(Box::new(block.clone()));
    let topic = Topic::new(
        init_connector.config.kafka_env.clone(),
        MessageType::FCT,
        init_connector.manifest.author.clone(),
        init_connector.manifest.name.clone(),
        init_connector.manifest.version.clone(),
        event_name,
    );
    let key = Key::new("ethereum".to_string(), "Block".to_string());

    // TODO: convert it to async function
    task::spawn_blocking(move || {
        // register protobuf schema
        init_connector.register_protobuf(MessageType::FCT, vec![Box::new(block.clone()), Box::new(transaction.clone())]);
    }).await?;

    // A Ws provider can be created from a ws(s) URI.
    // In case of wss you must add the "rustls" or "openssl" feature
    // to the ethers library dependency in `Cargo.toml`.
    let provider = Provider::<Ws>::connect(WSS_URL).await?;

    let mut stream = provider.subscribe_blocks().await?;
    while let Some(block) = stream.next().await {
        println!("block hash: {:?}", block.hash.unwrap());

        let b = build_block(block);
        let m = Message::new(topic.clone(), key.clone(), b.clone());
        let messages = vec![m];
        exec_connector.producer.produce_transactional_messages(messages).expect("TODO: panic message");
    }

    Ok(())
}

fn build_block(block: Block<H256>) -> ProtoBlock {
    let unix_time = u64::try_from(block.timestamp).unwrap();
    let time = SystemTime::UNIX_EPOCH + Duration::from_secs(unix_time);
    let timestamp = Timestamp::from(time);
    let proto_ts = protobuf::MessageField::some(timestamp);
    let hash = format!("{:#x}", block.hash.unwrap());

    let nonce = block.nonce.unwrap().to_low_u64_be();

    ProtoBlock {
        ts: proto_ts,
        Hash: hash,
        Difficulty: block.difficulty.as_u64(),
        Number: block.number.unwrap().as_u64(),
        GasLimit: block.gas_limit.as_u64(),
        GasUsed: block.gas_used.as_u64(),
        Nonce: nonce,
        // ignore this field
        special_fields: Default::default(),
    }
}