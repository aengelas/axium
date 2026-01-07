mod schema;

use anyhow::Result;
use axum::{Router, extract::State, http::HeaderMap, routing::get};
use clap::Parser;
use db::{Pool, RoPool, RwPool, diesel_async::RunQueryDsl};
use diesel::{
    QueryableByName,
    sql_types::{Integer, Text},
};
use std::{net::SocketAddr, ops::DerefMut};
use tokio::net::TcpListener;
use tracing::{debug, trace};

#[jacklog::verbose]
#[derive(Parser, Debug)]
struct Cli {
    #[clap(short, long)]
    database_url: String,

    #[clap(short, long, default_value = "127.0.0.1:8080")]
    listen: SocketAddr,
}

#[derive(Clone)]
struct AppState {
    ro_pool: RoPool,
    rw_pool: RwPool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    jacklog::from_level!(cli.verbose + 2)?;
    debug!(?cli);

    let state = AppState {
        rw_pool: Pool::rw_builder()
            .database_url(&cli.database_url)
            .test_mode(cfg!(test))
            .build()?,
        ro_pool: Pool::ro_builder()
            .database_url(&cli.database_url)
            .test_mode(cfg!(test))
            .build()?,
    };

    let app = Router::new()
        .route("/", get(root).post(record))
        .with_state(state);

    axum::serve(TcpListener::bind(cli.listen).await?, app).await?;

    Ok(())
}

#[derive(Debug, QueryableByName, PartialEq)]
#[diesel(table_name = schema::requests)]
struct Row {
    #[diesel(column_name = "id", sql_type = Integer)]
    id: i32,
    #[diesel(column_name = "user_agent", sql_type = Text)]
    user_agent: String,
}
async fn root(
    State(AppState {
        ro_pool,
        ..
    }): State<AppState>,
) -> String {
    let rows: Vec<Row> =
        db::diesel::sql_query("select id, user_agent from requests;")
            .load(ro_pool.get().await.unwrap().deref_mut())
            .await
            .unwrap();

    let mut buf = String::new();
    for row in rows {
        buf.push_str(&format!("{}\t{}\n", row.id, row.user_agent));
    }

    buf
}

#[axum::debug_handler]
async fn record(
    headers: HeaderMap,
    State(AppState {
        rw_pool,
        ..
    }): State<AppState>,
) -> &'static str {
    trace!(?headers);

    let user_agent = headers
        .get("user-agent")
        .expect("missing user-agent header");

    db::diesel::sql_query("insert into requests (user_agent) values ($1);")
        .bind::<Text, _>(format!("{user_agent:?}"))
        .execute(rw_pool.get().await.unwrap().deref_mut())
        .await
        .unwrap();

    "OK"
}

#[cfg(test)]
mod tests {
    use axum::http::header::USER_AGENT;

    use super::*;
    use std::env;

    #[tokio::test]
    async fn test_transactions() {
        let state = AppState {
            rw_pool: Pool::rw_builder()
                .database_url(env::var("DATABASE_URL").unwrap())
                // This is silly in a test, but you can include it in app code
                // as well.
                .test_mode(cfg!(test))
                .build()
                .unwrap(),
            ro_pool: Pool::ro_builder()
                .database_url(env::var("DATABASE_URL").unwrap())
                .test_mode(cfg!(test))
                .build()
                .unwrap(),
        };

        record(
            [(USER_AGENT, "THIS IS A TEST".parse().unwrap())]
                .into_iter()
                .collect(),
            State(state.clone()),
        )
        .await;

        let res = root(State(state)).await;

        assert_eq!(res, "");
    }
}
