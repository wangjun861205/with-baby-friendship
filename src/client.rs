use core::time;

use anyhow::Error;
use chrono::prelude::*;
use rdkafka::{
    producer::{FutureProducer as KafkaProducer, FutureRecord},
    util::Timeout,
};
use redis::{aio::Connection as RedisConnection, AsyncCommands};
use serde::Serialize;
use tokio;

pub async fn gen_key(redis: &mut RedisConnection, uid: i32) -> Result<String, Error> {
    let seq: String = redis.incr(format!("{}_seq", uid), 1).await?;
    Ok(format!("{}-{}", Utc::now().timestamp(), seq))
}

pub async fn request<T: Send + Serialize>(
    redis: &mut RedisConnection,
    kafka: &KafkaProducer,
    uid: i32,
    topic: String,
    req: T,
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
    let res: String = redis.blpop(key, 10).await?;
    Ok(res)
}

#[cfg(test)]
mod test {
    use rdkafka::client::Client as KafkaClient;
    use rdkafka::config::ClientConfig as KafkaConfig;
    use rdkafka::producer::FutureProducer as KafkaProducer;
    use redis::Client as RedisClient;

    #[tokio::test]
    async fn test_request() {
        let redis = RedisClient::open("redis://localhost").unwrap();
        let kafka = KafkaProducer::from(
            KafkaConfig::new()
                .set("bootstrap.servers", "localhost:9092")
                .create()
                .unwrap(),
        );
        super::request(
            &mut redis.get_async_connection().await.unwrap(),
            &kafka,
            1,
            "test".into(),
            "hello".to_owned(),
        )
        .await
        .unwrap();
    }
}
