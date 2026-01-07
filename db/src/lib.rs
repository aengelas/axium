use buildstructor::buildstructor;
use diesel::{PgConnection, r2d2::ConnectionManager};
use r2d2::ManageConnection;
use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

pub use diesel;

type R2d2Pool = diesel::r2d2::Pool<ConnectionManager<PgConnection>>;

pub type RwPool = Pool<ReadWrite>;
pub type RoPool = Pool<ReadOnly>;

#[derive(Clone, Copy, Debug)]
pub struct ReadOnly;

#[derive(Clone, Copy, Debug)]
pub struct ReadWrite;

#[derive(Clone, Debug)]
pub struct Pool<T> {
    pool: R2d2Pool,
    rw: PhantomData<T>,
}

#[buildstructor]
impl<T> Pool<T> {
    #[builder]
    pub fn new(
        database_url: String,
        test_mode: Option<bool>,
    ) -> Result<Self, r2d2::Error> {
        let manager = ConnectionManager::new(database_url);

        Ok(Self {
            pool: R2d2Pool::builder()
                .max_size({
                    #[cfg(test)]
                    {
                        1
                    }
                    #[cfg(not(test))]
                    {
                        10
                    }
                })
                .connection_customizer(if test_mode.unwrap_or(false) {
                    use diesel::r2d2::TestCustomizer;

                    Box::new(TestCustomizer)
                } else {
                    use diesel::r2d2::NopConnectionCustomizer;

                    Box::new(NopConnectionCustomizer)
                })
                .test_on_check_out(true)
                .build(manager)?,
            rw: PhantomData,
        })
    }

    pub fn get(&self) -> Result<PooledConnection<T>, r2d2::Error> {
        Ok(PooledConnection {
            conn: self.pool.get()?,
            rw: PhantomData,
        })
    }
}

pub struct PooledConnection<T> {
    conn: r2d2::PooledConnection<ConnectionManager<PgConnection>>,
    rw: PhantomData<T>,
}

impl<T> Deref for PooledConnection<T> {
    type Target =
        <ConnectionManager<PgConnection> as ManageConnection>::Connection;

    fn deref(&self) -> &Self::Target {
        &*self.conn
    }
}

impl<T> DerefMut for PooledConnection<T> {
    fn deref_mut(&mut self) -> &mut PgConnection {
        self.conn.deref_mut()
    }
}

#[buildstructor]
impl Pool<ReadWrite> {
    #[builder(entry = "rw_builder")]
    pub fn new_rw(
        database_url: String,
        test_mode: Option<bool>,
    ) -> Result<Self, r2d2::Error> {
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
    ) -> Result<Self, r2d2::Error> {
        Self::builder()
            .database_url(database_url)
            .and_test_mode(test_mode)
            .build()
    }
}
