use std::ops::Deref;

use crate::r2d2::RedisManager;
use crate::Outputer;
use r2d2::Pool;
use redis::{Commands, ToRedisArgs};
use serde::Serialize;
pub struct RedisOutput(Pool<RedisManager>);

impl RedisOutput {
    pub fn new(uri: &str) -> Result<Self, anyhow::Error> {
        Ok(Self(Pool::new(RedisManager::new(uri)?)?))
    }
}

impl Deref for RedisOutput {
    type Target = Pool<RedisManager>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Serialize)]
pub enum Response<T: Serialize> {
    Ok { data: T },
    Err { detail: String },
}

impl<K: ToRedisArgs, E: std::fmt::Display> Outputer<K, E, anyhow::Error> for RedisOutput {
    fn error(&self, key: K, err: E) -> Result<(), anyhow::Error> {
        let res = serde_json::to_string(&Response::<()>::Err {
            detail: format!("{}", err),
        })
        .unwrap();
        self.get()?.rpush(key, res)?;
        Ok(())
    }

    fn ok<T: Serialize>(&self, key: K, data: T) -> Result<(), anyhow::Error> {
        let res = serde_json::to_string(&Response::<T>::Ok { data: data }).unwrap();
        self.get()?.rpush(key, res)?;
        Ok(())
    }
}
