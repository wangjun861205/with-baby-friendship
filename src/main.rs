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
use redis::{self, Commands};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;
use std::str::from_utf8;
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

pub trait Outputer<'a, K, E, RE> {
    fn ok<T: Serialize>(
        &'a self,
        key: K,
        data: T,
    ) -> Pin<Box<dyn Future<Output = Result<(), RE>> + 'a>>;
    fn error(&'a self, key: K, err: E) -> Pin<Box<dyn Future<Output = Result<(), RE>> + 'a>>;
}

// impl<T: Serialize> Response<T> {
//     fn ok(data: T) -> Self {
//         Response::Ok { data }
//     }
//     fn error<E: std::fmt::Display>(err: E) -> Self {
//         Response::Err {
//             detail: format!("{}", err),
//         }
//     }
// }

async fn handle_message<
    'a,
    P: Persister<UID = i64>,
    C: Cacher<UID = i64>,
    L: Logger,
    O: Outputer<'a, &'a [u8], anyhow::Error, anyhow::Error>,
>(
    key: &'a [u8],
    body: &[u8],
    mgr: &FriendManager<P, C, L>,
    out: &'a O,
) -> Result<(), anyhow::Error> {
    let req = serde_json::from_slice::<Request>(body).context("failed to deserialize message")?;
    match req {
        Request::Add { uid_a, uid_b } => match mgr.add_friend(&uid_a, &uid_b).await {
            Err(e) => out.error(key, e).await,
            Ok(v) => out.ok(key, v).await,
        },
        Request::Delete { uid_a, uid_b } => match mgr.delete_friend(&uid_a, &uid_b).await {
            Err(e) => out.error(key, e).await,
            Ok(v) => out.ok(key, v).await,
        },
        Request::Friends { uid } => match mgr.query_friends(&uid).await {
            Err(e) => out.error(key, e).await,
            Ok(v) => out.ok(key, v).await,
        },
        Request::Recommendation { uid } => match mgr.recommendation(&uid).await {
            Err(e) => out.error(key, e).await,
            Ok(v) => out.ok(key, v).await,
        },
        Request::AddNode { uid } => match mgr.add_user(&uid).await {
            Err(e) => out.error(key, e).await,
            Ok(v) => out.ok(key, v).await,
        },
        Request::DeleteNode { uid } => match mgr.delete_user(&uid).await {
            Err(e) => out.error(key, e).await,
            Ok(v) => out.ok(key, v).await,
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
    let mgr = core::FriendManager::new(p, c, MyLogger {});
    let output = outputers::RedisOutput::new("redis://localhost").unwrap();
    loop {
        for ms in k_consumer.poll().expect("failed to poll kafka").iter() {
            for m in ms.messages() {
                if let Err(e) = handle_message(m.key, m.value, &mgr, &output).await {
                    println!("{}", e);
                }
                k_consumer.commit_consumed().unwrap();
            }
        }
    }
}
