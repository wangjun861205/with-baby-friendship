use crate::core::Cacher;
use crate::error::Error;
use redis::{AsyncCommands, Client, RedisError};
use serde_json;
pub struct Redis {
    client: Client,
}

impl Redis {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

impl Cacher for Redis {
    type UID = String;
    fn delete(&self, uid: Self::UID) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>>>> {
        let client = self.client.clone();
        Box::pin(async move {
            let mut conn = client.get_async_connection().await?;
            conn.del(format!("uid_{}", uid)).await?;
            Ok(())
        })
    }

    fn insert(&self, uid: Self::UID, friends: Vec<Self::UID>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>>>> {
        let client = self.client.clone();
        Box::pin(async move {
            let mut conn = client.get_async_connection().await?;
            conn.set(
                format!("uid_{}", uid),
                serde_json::to_string(&friends).map_err(|_| RedisError::from((redis::ErrorKind::TypeError, "invalid value")))?,
            )
            .await?;
            Ok(())
        })
    }

    fn query(&self, uid: Self::UID) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<Vec<Self::UID>>, Error>>>> {
        let client = self.client.clone();
        Box::pin(async move {
            let mut conn = client.get_async_connection().await?;
            if let Some(s) = conn.get::<_, Option<String>>(format!("uid_{}", uid)).await? {
                return Ok(serde_json::from_str(&s).map_err(|_| RedisError::from((redis::ErrorKind::TypeError, "failed to deserialize json string")))?);
            }
            Ok(None)
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use redis::Client;
    use tokio;

    #[tokio::test]
    async fn test_insert_cache() {
        let client = Client::open("redis://localhost").unwrap();
        let r = Redis::new(client);
        r.insert(1.to_string(), vec![2.to_string(), 3.to_string()]).await.unwrap();
        assert!(r.query(1.to_string()).await.unwrap().unwrap() == vec![2.to_string(), 3.to_string()]);
    }

    #[tokio::test]
    async fn test_delete_cache() {
        let client = Client::open("redis://localhost").unwrap();
        let r = Redis::new(client);
        r.insert(1.to_string(), vec![2.to_string(), 3.to_string()]).await.unwrap();
        assert!(r.query(1.to_string()).await.unwrap().unwrap() == vec![2.to_string(), 3.to_string()]);
        r.delete(1.to_string()).await.unwrap();
        assert!(r.query(1.to_string()).await.unwrap().is_none());
    }
}
