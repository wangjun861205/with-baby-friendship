use core::time;

use anyhow::Error;
use chrono::prelude::*;
use rdkafka::{
    producer::{
        future_producer::FutureProducerContext, FutureProducer as KafkaProducer, FutureRecord,
    },
    util::Timeout,
};
use redis::{aio::Connection as RedisConnection, AsyncCommands};
use serde::Serialize;
use tokio;

async fn gen_key(redis: &mut RedisConnection, uid: i64) -> Result<String, Error> {
    let seq: i32 = redis.incr(format!("{}_seq", uid), 1).await?;
    Ok(format!("{}-{}", Utc::now().timestamp(), seq))
}

pub async fn request<T: Send + Serialize>(
    redis: &mut RedisConnection,
    kafka: &KafkaProducer,
    uid: i64,
    topic: String,
    req: T,
    read_timeout: usize,
) -> Result<String, Error> {
    let key = gen_key(redis, uid).await?;
    let body = serde_json::to_string(&req)?;
    kafka
        .send(
            FutureRecord {
                topic: &topic,
                partition: None,
                payload: Some(&body),
                key: Some(&key),
                timestamp: None,
                headers: None,
            },
            Timeout::After(time::Duration::from_secs(10)),
        )
        .await
        .map_err(|e| Error::msg(e.0.to_string()))?;
    let mut res: Vec<String> = redis.blpop(key, read_timeout).await?;
    Ok(res.pop().unwrap())
}

use crate::Request;

pub async fn add_node(
    uid: i64,
    redis: &mut RedisConnection,
    kafka: &KafkaProducer,
) -> Result<String, Error> {
    request(
        redis,
        kafka,
        uid,
        "friendship".into(),
        Request::AddNode { uid: uid },
        10,
    )
    .await
}

#[cfg(test)]
mod test {
    use rdkafka::config::ClientConfig as KafkaConfig;
    use rdkafka::consumer::{Consumer, DefaultConsumerContext, StreamConsumer};
    use rdkafka::producer::FutureProducer as KafkaProducer;
    use rdkafka::util::TokioRuntime;
    use rdkafka::Message;
    use redis::{AsyncCommands, Client as RedisClient};
    use std::str::from_utf8;

    #[tokio::test]
    async fn test_request() {
        tokio::spawn(async move {
            let redis = RedisClient::open("redis://localhost").unwrap();
            let kafka: StreamConsumer<DefaultConsumerContext, TokioRuntime> = KafkaConfig::new()
                .set("bootstrap.servers", "localhost:12092")
                .set("group.id", "1")
                .create()
                .unwrap();
            kafka.subscribe(&["friendship"]).unwrap();
            let data = kafka.recv().await.unwrap().detach();
            let key = from_utf8(data.key().unwrap()).unwrap();
            let value = from_utf8(data.payload().unwrap()).unwrap();
            println!("key: {}, value: {}", key, value);
            redis
                .get_async_connection()
                .await
                .unwrap()
                .rpush::<&str, &str, ()>(key, value)
                .await
                .unwrap()
        });
        let redis = RedisClient::open("redis://localhost").unwrap();
        let kafka = KafkaProducer::from(
            KafkaConfig::new()
                .set("bootstrap.servers", "localhost:12092")
                .create()
                .unwrap(),
        );
        let res = super::request(
            &mut redis.get_async_connection().await.unwrap(),
            &kafka,
            1,
            "friendship".into(),
            "hello world".to_owned(),
            10,
        )
        .await
        .unwrap();
        println!("response: {}", res);
    }
}
