mod schema;

use anyhow::Result;
use async_graphql::{
    EmptyMutation, EmptySubscription, Object, Schema, http::GraphiQLSource,
};
use async_graphql_axum::GraphQL;
use axum::{
    Router,
    extract::State,
    http::HeaderMap,
    response::{Html, IntoResponse},
    routing::get,
};
use clap::Parser;
use db::{
    Pool, ReadOnly, ReadWrite, ReadableConnection, WriteableConnection,
    diesel_async::{
        AsyncConnection, RunQueryDsl, scoped_futures::ScopedFutureExt,
    },
};
use diesel::{
    QueryableByName,
    sql_types::{Integer, Text},
};
use std::net::SocketAddr;
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
    ro_pool: Pool<ReadOnly>,
    rw_pool: Pool<ReadWrite>,
}

#[derive(Debug)]
struct Query;

#[Object]
impl Query {
    async fn hello(&self) -> &'static str {
        "hi there"
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    jacklog::from_level!(cli.verbose + 2)?;
    debug!(?cli);

    let schema =
        Schema::build(Query, EmptyMutation, EmptySubscription).finish();

    let state = AppState {
        rw_pool: Pool::rw_builder()
            .database_url(&cli.database_url)
            .test_mode(cfg!(test))
            .and_max_size(if cfg!(test) {
                Some(1)
            } else {
                None
            })
            .build()?,
        ro_pool: Pool::ro_builder()
            .database_url(&cli.database_url)
            .test_mode(cfg!(test))
            .and_max_size(if cfg!(test) {
                Some(1)
            } else {
                None
            })
            .build()?,
    };

    let app = Router::new()
        .route("/", get(root).post(record))
        // Mount the graphql schema at /graphql.
        .route("/graphql", get(graphiql).post_service(GraphQL::new(schema)))
        .with_state(state);

    axum::serve(TcpListener::bind(cli.listen).await?, app).await?;

    Ok(())
}

/// Expose the graphiql interactive schema inspector.
async fn graphiql() -> impl IntoResponse {
    Html(GraphiQLSource::build().endpoint("graphql").finish())
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
    let rows = select(&mut ro_pool.get().await.unwrap()).await;

    let mut buf = String::new();
    for row in rows {
        buf.push_str(&format!("{}\t{}\n", row.id, row.user_agent));
    }

    buf
}
async fn select(conn: &mut impl ReadableConnection) -> Vec<Row> {
    db::diesel::sql_query("select id, user_agent from requests;")
        .load(conn)
        .await
        .unwrap()
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
    let user_agent = format!("{user_agent:?}");

    let mut conn = rw_pool.get().await.unwrap();
    insert(&mut conn, user_agent).await.unwrap();

    "OK"
}

async fn insert<T: WriteableConnection>(
    conn: &mut T,
    user_agent: String,
) -> std::result::Result<usize, diesel::result::Error> {
    conn.transaction(|conn| {
        async move {
            db::diesel::sql_query(
                "insert into requests (user_agent) values ($1);",
            )
            .bind::<Text, _>(user_agent)
            .execute(conn)
            .await
        }
        .scope_boxed()
    })
    .await
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
                .test_mode(true)
                .max_size(1)
                .build()
                .unwrap(),
            ro_pool: Pool::ro_builder()
                .database_url(env::var("DATABASE_URL").unwrap())
                .test_mode(true)
                .max_size(1)
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

        let res = root(State(state.clone())).await;
        assert_eq!(res, "");
    }

    #[tokio::test]
    async fn test_transactions_wo_axum() {
        let pool = Pool::rw_builder()
            .database_url(env::var("DATABASE_URL").unwrap())
            .test_mode(true)
            .max_size(1)
            .build()
            .unwrap();

        let mut conn = pool.get().await.unwrap();
        db::diesel::sql_query("insert into requests (user_agent) values ($1);")
            .bind::<Text, _>("TX_TEST".to_string())
            .execute(&mut conn)
            .await
            .unwrap();
        let res = select(&mut conn).await;

        assert_eq!(res.len(), 1);
    }
}
