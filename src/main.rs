mod cachers;
mod core;
mod persisters;

use crate::core::{Cacher, FriendManager, Logger, Persister};
use anyhow::Context;
use kafka::{
    client::KafkaClient,
    consumer::{Consumer, Message},
};
use neo4rs::Graph;
use redis::{self, Client, Commands, RedisError};
use serde::{Deserialize, Serialize};
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

impl<T: Serialize> Response<T> {
    fn ok(data: T) -> Self {
        Response::Ok { data }
    }
    fn error<E: std::fmt::Display>(err: E) -> Self {
        Response::Err {
            detail: format!("{}", err),
        }
    }
}

fn response<T: Serialize>(
    redis_client: &mut redis::Client,
    channel: &str,
    message: T,
) -> Result<(), anyhow::Error> {
    let s = serde_json::to_string(&message).context("failed to serialize message")?;
    redis_client
        .publish(channel, s)
        .context("failed to publish response to redis")
}

async fn handle_message<P: Persister<UID = i64>, C: Cacher<UID = i64>, L: Logger>(
    body: &[u8],
    mgr: &FriendManager<P, C, L>,
) -> Result<String, anyhow::Error> {
    let req = serde_json::from_slice::<Request>(body).context("failed to deserialize message")?;
    match req {
        Request::Add { uid_a, uid_b } => {
            mgr.add_friend(&uid_a, &uid_b).await?;
            Ok(serde_json::to_string(&Response::ok("ok"))?)
        }
        Request::Delete { uid_a, uid_b } => {
            mgr.delete_friend(&uid_a, &uid_b).await?;
            Ok(serde_json::to_string(&Response::ok("ok"))?)
        }
        Request::Friends { uid } => Ok(serde_json::to_string(&mgr.query_friends(&uid).await?)?),
        Request::Recommendation { uid } => {
            Ok(serde_json::to_string(&mgr.recommendation(&uid).await?)?)
        }
        Request::AddNode { uid } => {
            mgr.add_user(&uid).await?;
            Ok(serde_json::to_string(&Response::ok("ok"))?)
        }
        Request::DeleteNode { uid } => {
            mgr.delete_user(&uid).await?;
            Ok(serde_json::to_string(&Response::ok("ok"))?)
        }
    }
}

#[tokio::main]
async fn main() {
    dotenv::dotenv().expect("failed to load .env");
    let mut k_consumer = Consumer::from_hosts(vec!["192.168.3.11:12092".into()])
        .with_topic_partitions("friendship".into(), &vec![0])
        .create()
        .expect("failed to connect to kafka");
    let mut r_client =
        redis::Client::open("redis://localhost").expect("failed to connect to redis");
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
    loop {
        for ms in k_consumer.poll().expect("failed to poll kafka").iter() {
            for m in ms.messages() {
                if let Ok(key) = from_utf8(m.key) {
                    match handle_message(m.value, &mgr).await {
                        Err(e) => {
                            println!("{}", e);
                            if let Err(re) = r_client.publish::<_, _, ()>(
                                key,
                                serde_json::to_string(&Response::<()>::error(e)).unwrap(),
                            ) {
                                println!("{}", re);
                            }
                        }
                        Ok(resp) => {
                            println!("{}", resp);
                            if let Err(re) = r_client.publish::<_, _, ()>(key, resp) {
                                println!("{}", re);
                            }
                        }
                    }
                    k_consumer.commit_consumed().unwrap();
                }
            }
        }
    }
}
