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

impl<'a, K: ToRedisArgs + 'a, E: std::fmt::Display> Outputer<'a, K, E, anyhow::Error>
    for RedisOutput
{
    fn error(
        &'a self,
        key: K,
        err: E,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), anyhow::Error>> + 'a>> {
        let res = serde_json::to_string(&Response::<()>::Err {
            detail: format!("{}", err),
        })
        .unwrap();
        Box::pin(async move {
            self.get()?.rpush(key, res)?;
            Ok(())
        })
    }

    fn ok<T: Serialize>(
        &'a self,
        key: K,
        data: T,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), anyhow::Error>> + 'a>> {
        let res = serde_json::to_string(&Response::<T>::Ok { data: data }).unwrap();
        Box::pin(async move {
            self.get()?.rpush(key, res)?;
            Ok(())
        })
    }
}
