use crate::core::Cacher;
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
    type Error = RedisError;
    type UID = i64;
    fn delete<'a>(
        &'a self,
        uid: Self::UID,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Self::Error>> + 'a>> {
        Box::pin(async move {
            let mut conn = self.client.get_async_connection().await?;
            conn.del(format!("uid_{}", uid)).await?;
            Ok(())
        })
    }

    fn insert<'a>(
        &'a self,
        uid: Self::UID,
        friends: Vec<Self::UID>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Self::Error>> + 'a>> {
        Box::pin(async move {
            let mut conn = self.client.get_async_connection().await?;
            conn.set(
                format!("uid_{}", uid),
                serde_json::to_string(&friends).map_err(|_| {
                    RedisError::from((redis::ErrorKind::TypeError, "invalid value"))
                })?,
            )
            .await?;
            Ok(())
        })
    }

    fn query<'a>(
        &'a self,
        uid: Self::UID,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Option<Vec<Self::UID>>, Self::Error>> + 'a>,
    > {
        Box::pin(async move {
            let mut conn = self.client.get_async_connection().await?;
            if let Some(s) = conn
                .get::<_, Option<String>>(format!("uid_{}", uid))
                .await?
            {
                return Ok(serde_json::from_str(&s).map_err(|_| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "failed to deserialize json string",
                    ))
                })?);
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
        r.insert(1, vec![2, 3]).await.unwrap();
        assert!(r.query(1).await.unwrap().unwrap() == vec![2, 3]);
    }

    #[tokio::test]
    async fn test_delete_cache() {
        let client = Client::open("redis://localhost").unwrap();
        let r = Redis::new(client);
        r.insert(1, vec![2, 3]).await.unwrap();
        assert!(r.query(1).await.unwrap().unwrap() == vec![2, 3]);
        r.delete(1).await.unwrap();
        assert!(r.query(1).await.unwrap().is_none());
    }
}
