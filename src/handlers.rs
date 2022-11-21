use crate::core::{Cacher, Persister};
use crate::error::Error;
use actix_web::web::{Data, Json, Path};
use actix_web::HttpResponse;

async fn refresh_cache<P: Persister<UID = String>, C: Cacher<UID = String>>(persister: Data<P>, cacher: Data<C>, uid: String) -> Result<(), Error> {
    let friends = persister.friends(uid.clone()).await?;
    cacher.insert(uid, friends).await
}

pub async fn add_friend<P: Persister<UID = String>, C: Cacher<UID = String>>(persister: Data<P>, cacher: Data<C>, uids: Path<(String, String)>) -> Result<String, Error> {
    persister.insert(uids.0.clone(), uids.1.clone()).await?;
    refresh_cache(persister, cacher, uids.0.clone()).await?;
    Ok("ok".into())
}

pub async fn delete_friend<P: Persister<UID = String>, C: Cacher<UID = String>>(persister: Data<P>, cacher: Data<C>, uids: Path<(String, String)>) -> Result<String, Error> {
    persister.delete(uids.0.clone(), uids.1.clone()).await?;
    refresh_cache(persister, cacher, uids.0.clone()).await?;
    Ok("ok".into())
}

pub async fn query_friends<P: Persister<UID = String>, C: Cacher<UID = String>>(persister: Data<P>, cacher: Data<C>, uid: Path<(String,)>) -> Result<Json<Vec<String>>, Error> {
    if let Some(friends) = cacher.query(uid.0.clone()).await? {
        return Ok(Json(friends));
    }
    Ok(Json(persister.friends(uid.0.clone()).await?))
}

pub async fn recommendation<P: Persister<UID = String>, C: Cacher<UID = String>>(persister: Data<P>, cacher: Data<C>, uid: Path<(String,)>) -> Result<Json<Vec<String>>, Error> {
    Ok(Json(persister.recommendations(uid.0.clone(), 2, 3).await?))
}

pub async fn add_user<P: Persister<UID = String>, C: Cacher<UID = String>>(persister: Data<P>, cacher: Data<C>, uid: Path<(String,)>) -> Result<String, Error> {
    persister.insert_node(uid.0.clone()).await?;
    Ok("ok".into())
}

pub async fn delete_user<P: Persister<UID = String>, C: Cacher<UID = String>>(persister: Data<P>, cacher: Data<C>, uid: Path<(String,)>) -> Result<String, Error> {
    persister.delete_node(uid.0.clone()).await?;
    Ok("ok".into())
}

pub async fn is_friend<P: Persister<UID = String>, C: Cacher<UID = String>>(persister: Data<P>, cacher: Data<C>, uids: Path<(String, String)>) -> Result<HttpResponse, Error> {
    match persister.is_friend(uids.0.clone(), uids.1.clone()).await? {
        true => Ok(HttpResponse::Ok().finish()),
        _ => Ok(HttpResponse::NotFound().finish()),
    }
}
