use crate::core::Persister;
use neo4rs::{query, Graph};
use tokio;

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
    fn insert_node<'a>(
        &'a self,
        uid: Self::UID,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Self::Error>> + 'a>> {
        Box::pin(async move {
            self.graph
                .run(query("CREATE (:Person{ uid: $uid })").param("uid", uid))
                .await
        })
    }

    fn delete_node<'a>(
        &'a self,
        uid: Self::UID,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Self::Error>> + 'a>> {
        Box::pin(async move {
            self.graph
                .run(query("MATCH (p: Person{ uid: $uid }) DELETE p").param("uid", uid))
                .await
        })
    }
    fn exist_node<'a>(
        &'a self,
        uid: Self::UID,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool, Self::Error>> + 'a>> {
        Box::pin(async move {
            let mut rows = self.graph.execute(query("MATCH (p: Person{ uid: $uid } ) WITH count(p) > 0 AS node_exists RETURN node_exists")
            .param("uid", uid)).await?;
            if let Some(row) = rows.next().await? {
                return Ok(row.get("node_exists").unwrap());
            }
            unreachable!()
        })
    }
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
                        "MATCH (:Person{uid: $uid_a}) -[r:BE_FRIEND_OF]- (:Person{uid: $uid_b}) DELETE r",
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
                        "MATCH (:Person { uid: $uid }) -[:BE_FRIEND_OF]- (b:Person) RETURN b.uid AS uid ORDER BY uid",
                    )
                    .param("uid", uid),
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

    fn is_friend<'a>(
        &'a self,
        uid_a: Self::UID,
        uid_b: Self::UID,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool, Self::Error>> + 'a>> {
        Box::pin(async move {
            let mut rows = self.graph.execute(
                    query("MATCH (: Person { uid: $uid_a }) -[r: BE_FRIEND_OF]- (: Person { uid: $uid_b }) WITH count(r) > 0 AS is_friend RETURN is_friend")
                    .param("uid_a", uid_a)
                    .param("uid_b", uid_b)
                ).await?;
            if let Some(row) = rows.next().await? {
                if let Some(is_friend) = row.get("is_friend") {
                    return Ok(is_friend);
                }
            }
            unreachable!()
        })
    }

    fn insert<'a>(
        &'a self,
        uid_a: Self::UID,
        uid_b: Self::UID,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Self::Error>> + 'a>> {
        Box::pin(async move {
            self.graph.run(
                    query("MATCH (a:Person{ uid: $uid_a }), (b: Person{ uid: $uid_b }) CREATE (a) -[:BE_FRIEND_OF]-> (b)")
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
                    query(&format!(
                        "MATCH (a:Person) -[:BE_FRIEND_OF * {}]- (b:Person)
                        WITH a.uid AS src_uid, b.uid AS dst_uid, count(*) AS relative
                        WHERE src_uid = $uid AND relative >= $threshold
                        RETURN dst_uid",
                        level
                    ))
                    .param("uid", uid)
                    .param("threshold", threshold as i64),
                )
                .await?;
            let mut res = Vec::new();
            while let Some(row) = rows.next().await? {
                if let Some(uid) = row.get("dst_uid") {
                    res.push(uid);
                }
            }
            Ok(res)
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use dotenv;
    use neo4rs::Graph;

    #[tokio::test]
    async fn test_insert_node() {
        dotenv::dotenv().expect("failed to load environment variables");
        let username = dotenv::var("NEO4J_USERNAME").expect("failed to get NEO4J_USERNAME");
        let password = dotenv::var("NEO4J_PASSWORD").expect("failed to get NEO4J_PASSWORD");
        let graph = Graph::new("localhost:7687", &username, &password)
            .await
            .expect("failed to connect to neo4j");
        let neo = Neo::new(graph);
        neo.insert_node(1).await.expect("failed to insert node");
        assert!(
            neo.exist_node(1)
                .await
                .expect("failed to check node exists")
                == true
        );
        neo.delete_node(1).await.expect("failed to delete node");
    }

    #[tokio::test]
    async fn test_insert_relation() {
        dotenv::dotenv().expect("failed to load environment variables");
        let username = dotenv::var("NEO4J_USERNAME").expect("failed to get NEO4J_USERNAME");
        let password = dotenv::var("NEO4J_PASSWORD").expect("failed to get NEO4J_PASSWORD");
        let graph = Graph::new("localhost:7687", &username, &password)
            .await
            .expect("failed to connect to neo4j");
        let neo = Neo::new(graph);
        neo.insert_node(1).await.expect("failed to insert node");
        neo.insert_node(2).await.expect("failed to insert node");
        neo.insert(1, 2).await.expect("failed to insert relation");
        assert!(
            neo.is_friend(1, 2)
                .await
                .expect("failed to check is friend")
                == true
        );
        neo.delete(1, 2).await.expect("failed to delete relation");
        neo.delete_node(1).await.expect("failed to delete node");
        neo.delete_node(2).await.expect("failed to delete node");
    }

    #[tokio::test]
    async fn test_friends() {
        dotenv::dotenv().expect("failed to load environment variables");
        let username = dotenv::var("NEO4J_USERNAME").expect("failed to get NEO4J_USERNAME");
        let password = dotenv::var("NEO4J_PASSWORD").expect("failed to get NEO4J_PASSWORD");
        let graph = Graph::new("localhost:7687", &username, &password)
            .await
            .expect("failed to connect to neo4j");
        let neo = Neo::new(graph);
        neo.insert_node(1).await.expect("failed to insert node");
        neo.insert_node(2).await.expect("failed to insert node");
        neo.insert_node(3).await.expect("failed to insert node");
        neo.insert(1, 2).await.expect("failed to insert relation");
        neo.insert(1, 3).await.expect("failed to insert relation");
        let friends = neo.friends(1).await.expect("failed to get friends");
        assert!(friends == vec![2, 3]);
        neo.delete(1, 2).await.expect("failed to delete relation");
        neo.delete(1, 3).await.expect("failed to delete relation");
        neo.delete_node(1).await.expect("failed to delete node");
        neo.delete_node(2).await.expect("failed to delete node");
        neo.delete_node(3).await.expect("failed to delete node");
    }

    #[tokio::test]
    async fn test_recommendation() {
        dotenv::dotenv().expect("failed to load environment variables");
        let username = dotenv::var("NEO4J_USERNAME").expect("failed to get NEO4J_USERNAME");
        let password = dotenv::var("NEO4J_PASSWORD").expect("failed to get NEO4J_PASSWORD");
        let graph = Graph::new("localhost:7687", &username, &password)
            .await
            .expect("failed to connect to neo4j");
        let neo = Neo::new(graph);
        neo.insert_node(1).await.expect("failed to insert node");
        neo.insert_node(2).await.expect("failed to insert node");
        neo.insert_node(3).await.expect("failed to insert node");
        neo.insert_node(4).await.expect("failed to insert node");
        neo.insert(1, 2).await.expect("failed to insert relation");
        neo.insert(1, 3).await.expect("failed to insert relation");
        neo.insert(2, 4).await.expect("failed to insert relation");
        neo.insert(3, 4).await.expect("failed to insert relation");
        let rs = neo
            .recommendations(1, 2, 2)
            .await
            .expect("failed to get recommendation");
        neo.delete(1, 2).await.expect("failed to delete relation");
        neo.delete(1, 3).await.expect("failed to delete relation");
        neo.delete(2, 4).await.expect("failed to delete relation");
        neo.delete(3, 4).await.expect("failed to delete relation");
        neo.delete_node(1).await.expect("failed to delete node");
        neo.delete_node(2).await.expect("failed to delete node");
        neo.delete_node(3).await.expect("failed to delete node");
        neo.delete_node(4).await.expect("failed to delete node");
        assert!(rs == vec![4]);
    }
}
