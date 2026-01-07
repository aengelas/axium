use buildstructor::buildstructor;
use diesel_async::{
    AsyncConnection, AsyncPgConnection,
    pooled_connection::{
        AsyncDieselConnectionManager,
        deadpool::{self, BuildError, Hook, Object, PoolError},
    },
};
use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

pub use diesel;
pub use diesel_async;

pub type RwPool = Pool<ReadWrite>;
pub type RoPool = Pool<ReadOnly>;

#[derive(Clone, Copy, Debug)]
pub struct ReadOnly;

#[derive(Clone, Copy, Debug)]
pub struct ReadWrite;

#[derive(Clone)]
pub struct Pool<T> {
    pool: deadpool::Pool<AsyncPgConnection>,
    rw: PhantomData<T>,
}

#[buildstructor]
impl<T> Pool<T> {
    #[builder]
    pub fn new(
        database_url: String,
        test_mode: Option<bool>,
    ) -> Result<Self, BuildError> {
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            database_url,
        );

        let pool = deadpool::Pool::builder(manager)
            .post_create(Hook::async_fn(
                move |conn: &mut AsyncPgConnection, _metrics| {
                    Box::pin(async move {
                        if test_mode.unwrap_or(false) {
                            conn.begin_test_transaction().await.unwrap();
                        }

                        Ok(())
                    })
                },
            ))
            .build()?;

        Ok(Self {
            pool,
            rw: PhantomData,
        })
    }

    pub async fn get(&self) -> Result<PooledConnection<T>, PoolError> {
        Ok(PooledConnection {
            conn: self.pool.get().await?,
            rw: PhantomData,
        })
    }
}

#[buildstructor]
impl Pool<ReadWrite> {
    #[builder(entry = "rw_builder")]
    pub fn new_rw(
        database_url: String,
        test_mode: Option<bool>,
    ) -> Result<Self, BuildError> {
        Self::builder()
            .database_url(database_url)
            .and_test_mode(test_mode)
            .build()
    }
}

#[buildstructor]
impl Pool<ReadOnly> {
    #[builder(entry = "ro_builder")]
    pub fn new_ro(
        database_url: String,
        test_mode: Option<bool>,
    ) -> Result<Self, BuildError> {
        Self::builder()
            .database_url(database_url)
            .and_test_mode(test_mode)
            .build()
    }
}

pub struct PooledConnection<T> {
    conn: Object<AsyncPgConnection>,
    rw: PhantomData<T>,
}

impl<T> Deref for PooledConnection<T> {
    type Target = AsyncPgConnection;

    fn deref(&self) -> &Self::Target {
        &*self.conn
    }
}

impl<T> DerefMut for PooledConnection<T> {
    fn deref_mut(&mut self) -> &mut AsyncPgConnection {
        self.conn.deref_mut()
    }
}
