use std::time::{Duration, SystemTime};

use ethers::prelude::{Block, H256, Middleware, Provider, StreamExt, Ws};
use ethers::utils::hex::encode;
use eyre::Result;
use protobuf::well_known_types::timestamp::Timestamp;

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
    let mut connector = Connector::new();

    let event_name = topic::get_event_name(Box::new(block.clone()));
    let topic = Topic::new(
        connector.config.kafka_env.clone(),
        MessageType::FCT,
        connector.manifest.author.clone(),
        connector.manifest.name.clone(),
        connector.manifest.version.clone(),
        event_name,
    );
    let key = Key::new("ethereum".to_string(), "Block".to_string());

    // register protobuf schema
    connector.register_protos(MessageType::FCT, vec![Box::new(block.clone()), Box::new(transaction.clone())]).await;

    // A ws provider can be created from a ws(s) URI.
    // In case of wss you must add the "rustls" or "openssl" feature
    // to the ethers library dependency in `Cargo.toml`.
    let provider = Provider::<Ws>::connect(WSS_URL).await?;

    let mut stream = provider.subscribe_blocks().await?;
    while let Some(block) = stream.next().await {
        println!("block hash: {:?}", block.hash.unwrap());

        let b = build_block(&block);
        let m = Message::new(topic.clone(), key.clone(), b.clone());
        let messages = vec![m];
        connector.producer.produce_transactional_messages(messages).await.expect("failed to produce messages");
    }

    Ok(())
}

fn build_block(block: &Block<H256>) -> ProtoBlock {
    let unix_time = u64::try_from(block.timestamp).unwrap();
    let time = SystemTime::UNIX_EPOCH + Duration::from_secs(unix_time);
    let timestamp = Timestamp::from(time);
    let proto_ts = protobuf::MessageField::some(timestamp);
    let hash = encode(block.hash.unwrap().as_bytes());

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