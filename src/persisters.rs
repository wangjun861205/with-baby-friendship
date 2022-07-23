use crate::core::Persister;
use neo4rs::{query, Graph};
pub struct Neo {
    graph: Graph,
}

impl Neo {
    pub fn new(graph: Graph) -> Self {
        Self { graph }
    }
}

impl Persister for Neo {
    type Error = neo4rs::Error;
    type UID = i64;
    fn delete<'a>(
        &'a self,
        uid_a: Self::UID,
        uid_b: Self::UID,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Self::Error>> + 'a>> {
        Box::pin(async move {
            self
                .graph
                .run(
                    query(
                        "MATCH (:Person{uid: $uid_a}) -[r:BE_FRIEND_OF]- (:Person{uid: $uid_b) DELET r",
                    )
                    .param("uid_a", uid_a)
                    .param("uid_b", uid_b),
                )
                .await
        })
    }

    fn friends<'a>(
        &'a self,
        uid: Self::UID,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<Self::UID>, Self::Error>> + 'a>,
    > {
        Box::pin(async move {
            let mut rows = self
                .graph
                .execute(
                    query(
                        "MATCH (:Person { uid: $uid }) -[:BE_FRIEND_OF]- (b:Person) RETURN b.uid",
                    )
                    .param("uid", uid.to_string()),
                )
                .await?;
            let mut res = Vec::new();
            while let Some(r) = rows.next().await? {
                if let Some(uid) = r.get("uid") {
                    res.push(uid);
                }
            }
            Ok(res)
        })
    }

    fn insert<'a>(
        &'a self,
        uid_a: Self::UID,
        uid_b: Self::UID,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Self::Error>> + 'a>> {
        Box::pin(async move {
            self.graph.run(
                    query("MATCH (a:Person{ uid: $uid_a }), (b: Person{ uid: $uid_b }) CREATE (a) -[:BE_FRIEND_OF]- b")
                    .param("uid_a", uid_a)
                    .param("uid_b", uid_b)
                ).await
        })
    }

    fn recommendations<'a>(
        &'a self,
        uid: Self::UID,
        level: i32,
        threshold: i32,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<Self::UID>, Self::Error>> + 'a>,
    > {
        Box::pin(async move {
            let mut rows = self
                .graph
                .execute(
                    query(
                        r#"
                        MATCH (a:Person{ uid: $uid }) -> [:BE_FRIEND_OF*$level] -> (b:Person)
                        WITH b, count(*) as relation
                        WHERE relation > $threshold
                        RETURN b.uid
                    "#,
                    )
                    .param("uid", uid)
                    .param("level", level as i64)
                    .param("threshold", threshold as i64),
                )
                .await?;
            let mut res = Vec::new();
            while let Some(row) = rows.next().await? {
                if let Some(uid) = row.get("uid") {
                    res.push(uid);
                }
            }
            Ok(res)
        })
    }
}
