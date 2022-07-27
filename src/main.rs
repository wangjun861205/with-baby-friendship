mod cachers;
mod core;
mod outputers;
mod persisters;
// #[cfg(feature = "r2d2")]
mod r2d2;

use crate::core::{Cacher, FriendManager, Logger, Persister};
use anyhow::Context;
use kafka::consumer::Consumer;
use neo4rs::Graph;
use redis;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio;

#[derive(Serialize, Deserialize)]
struct MyLogger;

impl core::Logger for MyLogger {
    fn debug<'a>(&'a self, i: &'a dyn std::fmt::Display) {
        println!("{}", i);
    }

    fn error<'a>(&'a self, e: &'a dyn std::fmt::Display) {
        println!("{}", e);
    }

    fn info<'a>(&'a self, i: &'a dyn std::fmt::Display) {
        println!("{}", i);
    }

    fn warn<'a>(&'a self, w: &'a dyn std::fmt::Display) {
        println!("{}", w);
    }
}

#[derive(Deserialize)]
pub enum Request {
    Add { uid_a: i64, uid_b: i64 },
    Delete { uid_a: i64, uid_b: i64 },
    Friends { uid: i64 },
    Recommendation { uid: i64 },
    AddNode { uid: i64 },
    DeleteNode { uid: i64 },
}

#[derive(Debug, Serialize)]
pub enum Response<T: Serialize> {
    Ok { data: T },
    Err { detail: String },
}

pub trait Outputer<K, E, RE> {
    fn ok<T: Serialize>(&self, key: K, data: T) -> Result<(), RE>;
    fn error(&self, key: K, err: E) -> Result<(), RE>;
}

async fn handle_message<
    P: Persister<UID = i64> + Send,
    C: Cacher<UID = i64> + Send,
    L: Logger + Send,
    O: Outputer<Vec<u8>, anyhow::Error, anyhow::Error>,
>(
    key: Vec<u8>,
    body: Vec<u8>,
    mgr: &FriendManager<P, C, L>,
    out: &O,
) -> Result<(), anyhow::Error> {
    let req = serde_json::from_slice::<Request>(&body).context("failed to deserialize message")?;
    match req {
        Request::Add { uid_a, uid_b } => match mgr.add_friend(&uid_a, &uid_b).await {
            Err(e) => out.error(key, e),
            Ok(v) => out.ok(key, v),
        },
        Request::Delete { uid_a, uid_b } => match mgr.delete_friend(&uid_a, &uid_b).await {
            Err(e) => out.error(key, e),
            Ok(v) => out.ok(key, v),
        },
        Request::Friends { uid } => match mgr.query_friends(&uid).await {
            Err(e) => out.error(key, e),
            Ok(v) => out.ok(key, v),
        },
        Request::Recommendation { uid } => match mgr.recommendation(&uid).await {
            Err(e) => out.error(key, e),
            Ok(v) => out.ok(key, v),
        },
        Request::AddNode { uid } => match mgr.add_user(&uid).await {
            Err(e) => out.error(key, e),
            Ok(v) => out.ok(key, v),
        },
        Request::DeleteNode { uid } => match mgr.delete_user(&uid).await {
            Err(e) => out.error(key, e),
            Ok(v) => out.ok(key, v),
        },
    }
}

#[tokio::main]
async fn main() {
    dotenv::dotenv().expect("failed to load .env");
    let mut k_consumer = Consumer::from_hosts(vec!["192.168.3.11:12092".into()])
        .with_topic_partitions("friendship".into(), &vec![0])
        .create()
        .expect("failed to connect to kafka");
    let graph = Graph::new(
        &dotenv::var("NEO4J_ADDRESS").expect("NEO4J_ADDRESS not exists in .env"),
        &dotenv::var("NEO4J_USERNAME").expect("NEO4J_USERNAME not exists in .env"),
        &dotenv::var("NEO4J_PASSWORD").expect("NEO4J_PASSWORD not exists in .env"),
    )
    .await
    .expect("failed to connect to neo4j");
    let p = persisters::Neo::new(graph);
    let r = redis::Client::open("redis://localhost").expect("failed to connect to redis");
    let c = cachers::Redis::new(r);
    let mgr = Arc::new(core::FriendManager::new(p, c, MyLogger {}));
    let output = Arc::new(outputers::RedisOutput::new("redis://localhost").unwrap());
    loop {
        for ms in k_consumer.poll().expect("failed to poll kafka").iter() {
            for m in ms.messages() {
                let mgr = mgr.clone();
                let output = output.clone();
                let key = m.key.to_vec();
                let value = m.value.to_vec();
                tokio::spawn(async move {
                    if let Err(e) = handle_message(key, value, &mgr, output.as_ref()).await {
                        println!("{}", e);
                    }
                });
                k_consumer.commit_consumed().unwrap();
            }
        }
    }
}
