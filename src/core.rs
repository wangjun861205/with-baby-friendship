use crate::error::Error;
use std::future::Future;
use std::pin::Pin;

pub trait Persister {
    type UID;
    fn insert_node(&self, uid: Self::UID) -> Pin<Box<dyn Future<Output = Result<(), Error>>>>;
    fn delete_node(&self, uid: Self::UID) -> Pin<Box<dyn Future<Output = Result<(), Error>>>>;
    fn exist_node(&self, uid: Self::UID) -> Pin<Box<dyn Future<Output = Result<bool, Error>>>>;
    fn insert(&self, uid_a: Self::UID, uid_b: Self::UID) -> Pin<Box<dyn Future<Output = Result<(), Error>>>>;
    fn delete(&self, uid_a: Self::UID, uid_b: Self::UID) -> Pin<Box<dyn Future<Output = Result<(), Error>>>>;
    fn friends(&self, uid: Self::UID) -> Pin<Box<dyn Future<Output = Result<Vec<Self::UID>, Error>>>>;
    fn is_friend(&self, uid_a: Self::UID, uid_b: Self::UID) -> Pin<Box<dyn Future<Output = Result<bool, Error>>>>;
    fn recommendations(&self, uid: Self::UID, level: i32, threshold: i32) -> Pin<Box<dyn Future<Output = Result<Vec<Self::UID>, Error>>>>;
}

pub trait Cacher {
    type UID;
    fn insert(&self, uid: Self::UID, friends: Vec<Self::UID>) -> Pin<Box<dyn Future<Output = Result<(), Error>>>>;
    fn delete(&self, uid: Self::UID) -> Pin<Box<dyn Future<Output = Result<(), Error>>>>;
    fn query(&self, uid: Self::UID) -> Pin<Box<dyn Future<Output = Result<Option<Vec<Self::UID>>, Error>>>>;
}
