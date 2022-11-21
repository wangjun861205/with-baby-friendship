mod cachers;
mod client;
mod core;
mod error;
mod handlers;
mod outputers;
mod persisters;
mod r2d2;

use actix_web::{
    web::{delete, get, post, Data},
    App, HttpServer,
};
use cachers::Redis;
use env_logger;
use log::warn;
use neo4rs::Graph;
use persisters::Neo;
use redis;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
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

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();
    if let Err(e) = dotenv::dotenv() {
        warn!("{}", e);
    }
    let graph = Arc::new(
        Graph::new(
            &dotenv::var("NEO4J_ADDRESS").expect("NEO4J_ADDRESS not exists"),
            &dotenv::var("NEO4J_USERNAME").expect("NEO4J_USERNAME not exists"),
            &dotenv::var("NEO4J_PASSWORD").expect("NEO4J_PASSWORD not exists"),
        )
        .await
        .expect("failed to connect to neo4j"),
    );
    HttpServer::new(move || {
        let p = Neo::new(graph.clone());
        let r = redis::Client::open("redis://localhost").expect("failed to connect to redis");
        let c = Redis::new(r);
        App::new()
            .app_data(Data::new(p))
            .app_data(Data::new(c))
            .route("/users/{uid}", post().to(handlers::add_user::<Neo, Redis>))
            .route("/users/{uid_a}/friends/{uid_b}", post().to(handlers::add_friend::<Neo, Redis>))
            .route("/users/{uid_a}/friends/{uid_b}", delete().to(handlers::delete_friend::<Neo, Redis>))
            .route("/users/{uid}/friends", get().to(handlers::query_friends::<Neo, Redis>))
            .route("/users/{uid_a}/friends/{uid_b}", get().to(handlers::is_friend::<Neo, Redis>))
    })
    .bind(dotenv::var("ADDRESS").unwrap_or("0.0.0.0:8000".into()))?
    .run()
    .await
}
