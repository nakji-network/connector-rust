use std::process;
use std::time::Duration;

use log::{debug, error, info, warn};
use protobuf::MessageDyn;
use rdkafka::{
    error::{KafkaError, RDKafkaErrorCode},
    producer::{BaseRecord, DefaultProducerContext, Producer as KafkaProducer, ThreadedProducer},
    util::Timeout,
    ClientConfig,
};
use thiserror::Error;

use super::message::Message;

// the producer will wait for up to the given delay to allow other records to be sent so that the sends can be batched together
const KAFKA_PRODUCER_LINGER_MS: &str = "1000";

// the maximum amount of time the client will wait for the response of a request
const KAFKA_PRODUCER_REQUEST_TIMEOUT_MS: &str = "60000";

// (10 mins) default 60000 (1 min) https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html
const KAFKA_PRODUCER_TRANSACTION_TIMEOUT_MS: &str = "600000";

// default 1000 https://docs.confluent.io/2.0.0/clients/librdkafka/CONFIGURATION_8md.html
const KAFKA_PRODUCER_QUEUE_BUFFERING_MAX_MS: &str = "2000";

const KAFKA_COMPRESSION_CODEC: &str = "snappy";

const KAFKA_INIT_TRANSACTION_TIMEOUT: Timeout = Timeout::After(Duration::from_secs(3));
const KAFKA_COMMIT_TRANSACTION_TIMEOUT: Timeout = Timeout::After(Duration::from_secs(10));
const KAFKA_ABORT_TRANSACTION_TIMEOUT: Timeout = Timeout::After(Duration::from_secs(10));
const KAFKA_FLUSH_TIMEOUT: Timeout = Timeout::After(Duration::from_secs(15));

pub struct Producer {
    producer: ThreadedProducer<DefaultProducerContext>,
    transaction_initialized: bool,
}

#[derive(Error, Debug)]
pub enum ProducerError {
    #[error("failed to marshall protobuf message to bytes")]
    ConvertBytes(#[from] protobuf::Error),
    #[error("producer failed to send message to the topic {0}")]
    Send(String),
    #[error(transparent)]
    Kafka(#[from] KafkaError),
}

impl Producer {
    pub fn new(kafka_url: &str, transactional_id: &str) -> Self {
        let producer: ThreadedProducer<_> = ClientConfig::new()
            .set("bootstrap.servers", kafka_url)
            .set("transactional.id", transactional_id)
            .set("linger.ms", KAFKA_PRODUCER_LINGER_MS)
            .set("request.timeout.ms", KAFKA_PRODUCER_REQUEST_TIMEOUT_MS)
            .set(
                "transaction.timeout.ms",
                KAFKA_PRODUCER_TRANSACTION_TIMEOUT_MS,
            )
            .set(
                "queue.buffering.max.ms",
                KAFKA_PRODUCER_QUEUE_BUFFERING_MAX_MS,
            )
            .set("compression.codec", KAFKA_COMPRESSION_CODEC)
            .set("debug", "all")
            .create()
            .expect("producer creation error");

        Producer {
            producer,
            transaction_initialized: false,
        }
    }

    // TODO: add mutex?
    pub async fn produce_transactional_messages(
        &mut self,
        messages: Vec<Message>,
    ) -> Result<(), ProducerError> {
        if !self.transaction_initialized {
            println!("starting producer");
            self.start_producer()?;
            println!("started producer");
            self.transaction_initialized = true;
        }

        println!("starting transaction");
        self.producer.begin_transaction()?;
        println!("started transaction");

        for message in messages {
            let topic = message.topic;
            self.produce_message(
                &topic.to_string(),
                message.key.to_bytes(),
                message.protobuf_message,
            )?;
        }

        'retry: loop {
            let result = self
                .producer
                .commit_transaction(KAFKA_COMMIT_TRANSACTION_TIMEOUT);
            match result {
                Ok(_) => {
                    break 'retry;
                }
                Err(err) => match err {
                    KafkaError::Transaction(rd_err) if rd_err.is_retriable() => {
                        warn!("failed to commit transactions, retrying...");
                        continue;
                    }
                    e => {
                        let rd_err_code = e.rdkafka_error_code();
                        match rd_err_code {
                            Some(code) => match code {
                                RDKafkaErrorCode::ProducerFenced => {
                                    error!("producer is fenced");
                                    self.close();
                                    break 'retry;
                                }
                                RDKafkaErrorCode::InvalidTransactionTimeout => {
                                    warn!("failed to commit transactions, timed out");
                                    break 'retry;
                                }
                                _ => {
                                    error!("failed to commit transactions, aborting...");
                                    self.producer
                                        .abort_transaction(KAFKA_ABORT_TRANSACTION_TIMEOUT)
                                        .expect("failed to abort transaction, killing producer..");
                                    break 'retry;
                                }
                            },
                            None => {
                                error!("failed to commit transactions, aborting...");
                                self.producer
                                    .abort_transaction(KAFKA_ABORT_TRANSACTION_TIMEOUT)
                                    .expect("failed to abort transaction, killing producer..");
                                break 'retry;
                            }
                        }
                    }
                },
            }
        }

        debug!("successfully committed transactions");
        Ok(())
    }

    fn close(&self) {
        info!("flushing outstanding Kafka messages");

        self.producer
            .flush(KAFKA_FLUSH_TIMEOUT)
            .expect("failed to flush messages");
        self.producer
            .abort_transaction(KAFKA_ABORT_TRANSACTION_TIMEOUT)
            .expect("failed to abort transaction, killing producer..");
        process::exit(1);
    }

    fn start_producer(&self) -> Result<(), ProducerError> {
        self.producer
            .init_transactions(KAFKA_INIT_TRANSACTION_TIMEOUT)?;
        Ok(())
    }

    // TODO: otel metrics and prometheus
    fn produce_message(
        &self,
        topic: &str,
        key: Vec<u8>,
        message: Box<dyn MessageDyn>,
    ) -> Result<(), ProducerError> {
        let out_bytes: Vec<u8> = message.write_to_bytes_dyn()?;

        let base_record = BaseRecord::to(topic).key(&key).payload(&out_bytes);

        if self.producer.send(base_record).is_err() {
            return Err(ProducerError::Send(topic.to_string()));
        }
        Ok(())
    }
}
