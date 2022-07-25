use kafka;
use r2d2::ManageConnection;
use redis::{self, Commands};

pub struct KafkaManager {
    hosts: Vec<String>,
}

impl KafkaManager {
    pub fn new(hosts: Vec<String>) -> Self {
        Self { hosts }
    }
}

impl ManageConnection for KafkaManager {
    type Connection = kafka::client::KafkaClient;
    type Error = kafka::Error;
    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let mut client = kafka::client::KafkaClient::new(self.hosts.clone());
        client.load_metadata_all()?;
        Ok(client)
    }
    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        false
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.load_metadata_all()
    }
}

pub struct RedisManager {
    client: redis::Client,
}

impl RedisManager {
    pub fn new(uri: &str) -> Result<Self, redis::RedisError> {
        let client = redis::Client::open(uri)?;
        Ok(Self { client })
    }
}

impl ManageConnection for RedisManager {
    type Connection = redis::Connection;
    type Error = redis::RedisError;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.client.get_connection()
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.acl_whoami()
    }
}
