use axum::{
    routing::{get, post},
    http::StatusCode,
    response::IntoResponse,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use arrowdb::db::ArrowDB;
use axum::extract::State;
use anyhow::Result;

#[tokio::main]
async fn main() {
    let db = ArrowDB::new("data");
    let app = Router::new()
        .route("/collection/list", get(list))
        .route("/collection/add", post(add_collection))
        .route("/arrow/insert", post(insert))
        .route("/arrow/query", post(query))
        .with_state(db);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8088").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn list(State(db): State<ArrowDB>,) -> Json<Vec<String>> {
    Json(db.get_collections())
}

async fn add_collection(State(db): State<ArrowDB>, Json(payload): Json<CreateCollection>,) -> &'static str {
    if db.create_collection(&payload.name, payload.dimension).is_ok() {
        "{\"code\": 0 }"
    } else { "\"code\": -1 }" }
}

async fn insert(State(db): State<ArrowDB>, Json(payload): Json<Arrow>,) ->Json<ArrowResponse> {
    let mut resp = ArrowResponse{code: -1, msg: None, id: None};
    let dim = payload.arrow.len();
    match db.get_hnsw(&payload.collection, dim) {
        Ok(hn)=> {
            match hn.insert(payload.arrow) {
                Ok(id)=> {
                    resp.code = 0;
                    resp.id = Some(id);
                }
                Err(e)=> {
                    resp.msg = Some(format!("{:?}", e));
                }
            }
        }
        Err(e)=> {
            resp.msg = Some(format!("{:?}", e));
        }
    }
    Json(resp)
}

async fn query(State(db): State<ArrowDB>, Json(payload): Json<ArrowQuery>,) ->Json<QueryResponse> {
    let mut resp = QueryResponse{code: -1, msg: None, ids: Vec::new()};
    let dim = payload.arrow.len();
    match db.get_hnsw(&payload.collection, dim) {
        Ok(hn)=> {
            match hn.search(payload.arrow, payload.number) {
                Ok(ids)=> {
                    resp.code = 0;
                    resp.ids = ids;
                }
                Err(e)=> {
                    resp.msg = Some(format!("{:?}", e));
                }
            }
        }
        Err(e)=> {
            resp.msg = Some(format!("{:?}", e));
        }
    }
    Json(resp)
}

#[derive(Deserialize)]
struct CreateCollection {
    name: String,
    dimension: usize,
}

#[derive(Deserialize)]
struct Arrow {
    collection: String,
    arrow: Vec<f32>
}

#[derive(Deserialize)]
struct ArrowQuery {
    collection: String,
    number: usize,
    arrow: Vec<f32>
}

#[derive(Serialize)]
struct ArrowResponse {
    code: i64,
    id: Option<u64>,
    msg: Option<String>
}

#[derive(Serialize)]
struct QueryResponse {
    code: i64,
    ids: Vec<(u64, f32)>,
    msg: Option<String>
}
