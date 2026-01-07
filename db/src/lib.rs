//! Thin wrapper around diesel-async/deadpool_postgres that provides 2 primary
//! features:
//!
//! 1. Read/Write differentiation, captured in the pool type;
//! 2. Ability to switch to "test mode" if constructing a pool for testing that
//!    runs everything in a transaction that gets rolled back by default.

use buildstructor::buildstructor;
use diesel_async::{
    AsyncConnection, AsyncPgConnection,
    pooled_connection::{
        AsyncDieselConnectionManager,
        deadpool::{self, BuildError, Hook, Object, PoolError},
    },
    scoped_futures::{ScopedBoxFuture, ScopedFutureExt},
};
use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
    thread,
};

/// Re-export diesel for use by consumers.
pub use diesel;

/// Re-export diesel_async for use by consumers.
pub use diesel_async;

/// Unit-struct to mark pools and connections as read-only.
#[derive(Clone, Copy, Debug)]
pub struct ReadOnly;

/// Unit-struct to mark pools and connections as read-write.
#[derive(Clone, Copy, Debug)]
pub struct ReadWrite;

/// Our own pool type that "taints" a pool with whether it's read/write or
/// read-only.
#[derive(Clone)]
pub struct Pool<T> {
    /// Underlying deadpool pool that actually manages the connections.
    pool: deadpool::Pool<AsyncPgConnection>,

    /// Marker to keep track of whether this is read/write or read-only.
    rw: PhantomData<T>,
}

#[buildstructor]
impl<T: Send> Pool<T> {
    /// Build a new pool, optionally enabling "test mode", which will run all
    /// queries in transactions to ensure a clean database, max pool size, etc.
    #[builder]
    pub fn new(
        database_url: String,
        test_mode: Option<bool>,
        max_size: Option<usize>,
    ) -> Result<Self, BuildError> {
        // The manager is responsible for knowing how to get a "thing" from the
        // pool. In this case, postgres connections.
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            database_url,
        );

        let pool = deadpool::Pool::builder(manager)
            .max_size(max_size.unwrap_or(
                // By default, we use 2x the number of threads visible
                // to the process.
                2 * thread::available_parallelism().unwrap().get(),
            ))
            .post_create(Hook::async_fn(
                // This post-create hook will enable the test
                // transaction mode when running tests. This keeps the
                // DB clean during multiple or parallel test runs.
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

    /// This wraps the get method from the deadpool pool and puts the returned
    /// connection into a type that tracks whether it's read-only or read/write.
    pub async fn get(
        &self,
    ) -> Result<PooledConnection<T, Object<AsyncPgConnection>>, PoolError> {
        Ok(PooledConnection {
            conn: self.pool.get().await?,
            rw: PhantomData,
        })
    }
}

#[buildstructor]
impl Pool<ReadWrite> {
    /// Convenience constructor to allow building a read/write pool.
    #[builder(entry = "rw_builder")]
    pub fn new_rw(
        database_url: String,
        test_mode: Option<bool>,
        max_size: Option<usize>,
    ) -> Result<Self, BuildError> {
        Self::builder()
            .database_url(database_url)
            .and_test_mode(test_mode)
            .and_max_size(max_size)
            .build()
    }
}

#[buildstructor]
impl Pool<ReadOnly> {
    /// Convenience constructor to allow building a read-only pool.
    #[builder(entry = "ro_builder")]
    pub fn new_ro(
        database_url: String,
        test_mode: Option<bool>,
        max_size: Option<usize>,
    ) -> Result<Self, BuildError> {
        Self::builder()
            .database_url(database_url)
            .and_test_mode(test_mode)
            .and_max_size(max_size)
            .build()
    }
}

/// The type of connections that our pools return, marked as read/write or
/// read-only. It implements various traits like Deref, DerefMut, AsMut to allow
/// seamlessly using this in place of a native diesel connection.
pub struct PooledConnection<T: Send, C: DerefMut + Send>
where
    C::Target: AsyncConnection,
{
    conn: C,
    rw: PhantomData<T>,
}

impl<T: Send> PooledConnection<T, Object<AsyncPgConnection>> {
    /// This is probably the gnarliest bit of this library. This allows passing
    /// a closure/function that uses a PooledConnection into one of our
    /// type-tainted connections. This ensures that even when using
    /// transactions, you need to have the right sort of connection available in
    /// order to call your function.
    pub async fn transaction<'a, 'conn, R, E, F>(
        &'conn mut self,
        callback: F,
    ) -> Result<R, E>
    where
        // This is the callback function you provide, that basically takes a
        // connection, and returns a result.
        F: for<'r> FnOnce(
                &'r mut PooledConnection<T, &mut AsyncPgConnection>,
            ) -> ScopedBoxFuture<'a, 'r, Result<R, E>>
            + Send
            + 'a,
        // The error in the result must impl From for diesel errors.
        E: From<diesel::result::Error> + Send + 'a,
        // The returned value must be Send.
        R: Send + 'a,
        'a: 'conn,
    {
        // Start a transaction via the AsyncPgConnection...
        self.conn
            .transaction(|conn| {
                // ... wrap the new &mut AsyncPgConnection that's in a
                // transaction into our PooledConnection to preserve read/write
                // vs. read-only context.
                let mut conn = PooledConnection::<T, &mut AsyncPgConnection> {
                    conn,
                    rw: PhantomData,
                };

                // Call the provided callback using our own PooledConnection.
                async move { callback(&mut conn).await }.scope_boxed()
            })
            .await
    }
}

/// Implement Deref for our connections we get from the pool. This allows using
/// these r/w-tainted connections natively with diesel functions.
impl<T: Send> Deref for PooledConnection<T, Object<AsyncPgConnection>> {
    type Target = AsyncPgConnection;

    fn deref(&self) -> &Self::Target {
        &*self.conn
    }
}

/// Implement DerefMut for our connections we get from the pool. This allows
/// using these r/w-tainted connections natively with diesel functions.
impl<T: Send> DerefMut for PooledConnection<T, Object<AsyncPgConnection>> {
    fn deref_mut(&mut self) -> &mut AsyncPgConnection {
        self.conn.deref_mut()
    }
}

/// Implement AsMut, which allows "cheap mutable-to-mutable reference
/// conversion". It's suggested if you implement DerefMut.
impl<T: Send, U> AsMut<U> for PooledConnection<T, Object<AsyncPgConnection>>
where
    <PooledConnection<T, Object<AsyncPgConnection>> as Deref>::Target: AsMut<U>,
{
    fn as_mut(&mut self) -> &mut U {
        self.deref_mut().as_mut()
    }
}

/// Implement Deref for mutable borrows of a connection. This is a no-op, but is
/// part of the glue that allows us to use our ReadableConnection and
/// WriteableConnection traits across both pool connections and raw connections
/// (which we have to work with in transactions).
impl<T: Send> Deref for PooledConnection<T, &mut AsyncPgConnection> {
    type Target = AsyncPgConnection;

    fn deref(&self) -> &Self::Target {
        self.conn
    }
}

/// Implement DerefMut for mutable borrows of a connection. This is a no-op, but
/// is part of the glue that allows us to use our ReadableConnection and
/// WriteableConnection traits across both pool connections and raw connections
/// (which we have to work with in transactions).
impl<T: Send> DerefMut for PooledConnection<T, &mut AsyncPgConnection> {
    fn deref_mut(&mut self) -> &mut AsyncPgConnection {
        self.conn
    }
}

/// Implement AsMut for mutable borrows of a connection. This is a no-op, but
/// is part of the glue that allows us to use our ReadableConnection and
/// WriteableConnection traits across both pool connections and raw connections
/// (which we have to work with in transactions).
impl<T: Send> AsMut<AsyncPgConnection>
    for PooledConnection<T, &mut AsyncPgConnection>
{
    fn as_mut(&mut self) -> &mut AsyncPgConnection {
        self.conn
    }
}

/// Mark a connection as usable for reads. It says nothing about whether you can
/// write to it.
pub trait ReadableConnection:
    Deref<Target = AsyncPgConnection> + DerefMut + Send
{
}

/// Blanket implementation of ReadableConnection for any connection that's
/// writeable.
impl<T> ReadableConnection for T where T: WriteableConnection {}

/// Connections straight from the read-only pool are readable.
impl ReadableConnection
    for PooledConnection<ReadOnly, Object<AsyncPgConnection>>
{
}

/// Borrowed connections from read-only pools that we deal with in transactions
/// are readable.
impl ReadableConnection for PooledConnection<ReadOnly, &mut AsyncPgConnection> {}

/// Mark a connection as usable for writes. Write connections can also be used
/// for reads.
pub trait WriteableConnection:
    Deref<Target = AsyncPgConnection> + DerefMut + Send
{
}

/// Connections from write pools are usable as WriteableConnections.
impl WriteableConnection
    for PooledConnection<ReadWrite, Object<AsyncPgConnection>>
{
}

/// Borrowed connections from write pools are usable as WriteableConnections.
impl WriteableConnection
    for PooledConnection<ReadWrite, &mut AsyncPgConnection>
{
}
