use anyhow::Context;
use serde::Serialize;
use std::error::Error;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;

pub trait Persister {
    type UID;
    type Error: Debug + Send + Sync + 'static;
    fn insert_node<'a>(
        &'a self,
        uid: Self::UID,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + 'a>>;
    fn delete_node<'a>(
        &'a self,
        uid: Self::UID,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + 'a>>;

    fn exist_node<'a>(
        &'a self,
        uid: Self::UID,
    ) -> Pin<Box<dyn Future<Output = Result<bool, Self::Error>> + 'a>>;

    fn insert<'a>(
        &'a self,
        uid_a: Self::UID,
        uid_b: Self::UID,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + 'a>>;

    fn delete<'a>(
        &'a self,
        uid_a: Self::UID,
        uid_b: Self::UID,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + 'a>>;

    fn friends<'a>(
        &'a self,
        uid: Self::UID,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Self::UID>, Self::Error>> + 'a>>;

    fn is_friend<'a>(
        &'a self,
        uid_a: Self::UID,
        uid_b: Self::UID,
    ) -> Pin<Box<dyn Future<Output = Result<bool, Self::Error>> + 'a>>;

    fn recommendations<'a>(
        &'a self,
        uid: Self::UID,
        level: i32,
        threshold: i32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Self::UID>, Self::Error>> + 'a>>;
}

pub trait Cacher {
    type UID;
    type Error: Error + Send + Sync + 'static;

    fn insert<'a>(
        &'a self,
        uid: Self::UID,
        friends: Vec<Self::UID>,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + 'a>>;
    fn delete<'a>(
        &'a self,
        uid: Self::UID,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + 'a>>;
    fn query<'a>(
        &'a self,
        uid: Self::UID,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Vec<Self::UID>>, Self::Error>> + 'a>>;
}

pub trait Logger {
    fn debug<'a>(&'a self, i: &'a dyn std::fmt::Display);
    fn info<'a>(&'a self, i: &'a dyn std::fmt::Display);
    fn warn<'a>(&'a self, w: &'a dyn std::fmt::Display);
    fn error<'a>(&'a self, e: &'a dyn std::fmt::Display);
}

pub struct FriendManager<P: Persister, C: Cacher, L: Logger> {
    persister: P,
    cacher: C,
    logger: L,
}

impl<U: Sized + Clone, P: Persister<UID = U>, C: Cacher<UID = U>, L: Logger>
    FriendManager<P, C, L>
{
    pub fn new(persister: P, cacher: C, logger: L) -> Self {
        Self {
            persister,
            cacher,
            logger,
        }
    }

    async fn refresh_cache(&self, uid: U) -> Result<(), anyhow::Error> {
        let friends =
            self.persister.friends(uid.clone()).await.map_err(|e| {
                anyhow::Error::msg(format!("{:?}", e)).context("failed to add friend")
            })?;
        self.cacher
            .insert(uid.clone(), friends)
            .await
            .context("failed to refresh cache")
    }

    pub async fn add_friend(&self, uid_a: &U, uid_b: &U) -> Result<(), anyhow::Error> {
        self.persister
            .insert(uid_a.clone(), uid_b.clone())
            .await
            .map_err(|e| anyhow::Error::msg(format!("{:?}", e)).context("failed to add friend"))?;
        self.refresh_cache(uid_a.clone())
            .await
            .context("failed to add friend")
    }

    pub async fn delete_friend(&self, uid_a: &U, uid_b: &U) -> Result<(), anyhow::Error> {
        self.persister
            .delete(uid_a.clone(), uid_b.clone())
            .await
            .map_err(|e| {
                anyhow::Error::msg(format!("{:?}", e)).context("failed to delete friend")
            })?;
        self.refresh_cache(uid_a.clone())
            .await
            .context("failed to delete friend")
    }

    pub async fn query_friends(&self, uid: &U) -> Result<Vec<U>, anyhow::Error> {
        if let Some(friends) = self
            .cacher
            .query(uid.clone())
            .await
            .context("failed to query friends")?
        {
            return Ok(friends);
        }
        Ok(self.persister.friends(uid.clone()).await.map_err(|e| {
            anyhow::Error::msg(format!("{:?}", e)).context("failed to query friends")
        })?)
    }

    pub async fn recommendation(&self, uid: &U) -> Result<Vec<U>, anyhow::Error> {
        self.persister
            .recommendations(uid.clone(), 2, 3)
            .await
            .map_err(|e| {
                anyhow::Error::msg(format!("{:?}", e)).context("failed to get recommendation")
            })
    }

    pub async fn add_user(&self, uid: &U) -> Result<(), anyhow::Error> {
        self.persister
            .insert_node(uid.clone())
            .await
            .map_err(|e| anyhow::Error::msg(format!("{:?}", e)))
    }

    pub async fn delete_user(&self, uid: &U) -> Result<(), anyhow::Error> {
        self.persister
            .delete_node(uid.clone())
            .await
            .map_err(|e| anyhow::Error::msg(format!("{:?}", e)).context("failed to add user"))
    }
}
