use std::{collections::HashMap, num::NonZeroUsize, sync::Arc, time::Duration};

use crate::{
    exceptions::rust_err::{ScyllaPyError, ScyllaPyResult},
    execution_profiles::ScyllaPyExecutionProfile,
    inputs::{BatchInput, ExecuteInput, PrepareInput},
    prepared_queries::ScyllaPyPreparedQuery,
    query_results::{
        ScyllaPyIterablePagedQueryResult, ScyllaPyIterableQueryResult, ScyllaPyQueryResult,
        ScyllaPyQueryReturns,
    },
    utils::{parse_python_query_params, scyllapy_future},
};
use openssl::{
    ssl::{SslContextBuilder, SslMethod, SslVerifyMode},
    x509::X509,
};
use pyo3::{pyclass, pymethods, types::PyList, PyAny, Python};
use scylla::{
    batch::BatchStatement,
    frame::{response::result::ColumnSpec, value::ValueList},
    prepared_statement::PreparedStatement,
    query::Query,
};

#[pyclass(frozen, weakref)]
#[derive(Clone)]
pub struct Scylla {
    contact_points: Vec<String>,
    username: Option<String>,
    password: Option<String>,
    keyspace: Option<String>,
    ssl_cert: Option<String>,
    connection_timeout: Option<u64>,
    write_coalescing: Option<bool>,
    disallow_shard_aware_port: Option<bool>,
    pool_size_per_host: Option<NonZeroUsize>,
    pool_size_per_shard: Option<NonZeroUsize>,
    keepalive_interval: Option<u64>,
    keepalive_timeout: Option<u64>,
    tcp_keepalive_interval: Option<u64>,
    tcp_nodelay: Option<bool>,
    default_execution_profile: Option<ScyllaPyExecutionProfile>,
    scylla_session: Arc<tokio::sync::RwLock<Option<scylla::Session>>>,
    column_specs: Arc<tokio::sync::RwLock<HashMap<String, Vec<ColumnSpec>>>>,
}

impl Scylla {
    /// Execute a query.
    ///
    /// This function is not exposed to python
    /// and used to execute queries from rust code.
    ///
    /// The main reason of using separate method is
    /// an ability to use generic parameters in this function.
    ///
    /// # Errors
    ///
    /// May raise an error if driver
    /// fails to execute query.
    pub fn native_execute<'a>(
        &'a self,
        py: Python<'a>,
        query: Option<impl Into<Query> + Send + 'static>,
        prepared: Option<PreparedStatement>,
        values: impl ValueList + Send + 'static + Sync,
        paged: i32,
    ) -> ScyllaPyResult<&'a PyAny> {
        let session_arc = self.scylla_session.clone();
        scyllapy_future(py, async move {
            let session_guard = session_arc.read().await;
            let session = session_guard.as_ref().ok_or(ScyllaPyError::SessionError(
                "Session is not initialized.".into(),
            ))?;
            if paged > 1 {
                match (query, prepared) {
                    (Some(query), None) => {
                        let mut query_s = query.into();
                        query_s.set_page_size(paged);
                        Ok(ScyllaPyQueryReturns::IterablePagedQueryResult(
                            ScyllaPyIterablePagedQueryResult::new(
                                session.query_iter(query_s, values.serialized()?).await?,
                                paged,
                            ),
                        ))
                    }
                    (None, Some(mut prepared)) => {
                        prepared.set_page_size(paged);
                        Ok(ScyllaPyQueryReturns::IterablePagedQueryResult(
                            ScyllaPyIterablePagedQueryResult::new(
                                session.execute_iter(prepared, values.serialized()?).await?,
                                paged,
                            ),
                        ))
                    }
                    _ => Err(ScyllaPyError::SessionError(
                        "You should pass either query or prepared query.".into(),
                    )),
                }
            } else if paged == 1 {
                match (query, prepared) {
                    (Some(query), None) => Ok(ScyllaPyQueryReturns::IterableQueryResult(
                        ScyllaPyIterableQueryResult::new(
                            session.query_iter(query, values.serialized()?).await?,
                        ),
                    )),
                    (None, Some(prepared)) => Ok(ScyllaPyQueryReturns::IterableQueryResult(
                        ScyllaPyIterableQueryResult::new(
                            session.execute_iter(prepared, values.serialized()?).await?,
                        ),
                    )),
                    _ => Err(ScyllaPyError::SessionError(
                        "You should pass either query or prepared query.".into(),
                    )),
                }
            } else {
                match (query, prepared) {
                    (Some(query), None) => Ok(ScyllaPyQueryReturns::QueryResult(
                        ScyllaPyQueryResult::new(session.query(query, values.serialized()?).await?),
                    )),
                    (None, Some(prepared)) => {
                        Ok(ScyllaPyQueryReturns::QueryResult(ScyllaPyQueryResult::new(
                            session.execute(&prepared, values.serialized()?).await?,
                        )))
                    }
                    _ => Err(ScyllaPyError::SessionError(
                        "You should pass either query or prepared query.".into(),
                    )),
                }
            }
        })
        .map_err(Into::into)
    }

    pub fn get_col_specs<'a>(
        &'a self,
        py: Python<'a>,
        name: String,
        columns: Option<Vec<String>>,
    ) -> Option<Vec<ColumnSpec>> {
        let column_specs = self.column_specs.clone();
        let col_specs = column_specs.try_read().unwrap();
        if !col_specs.contains_key(&name) {
            let _ = self.refresh_column_specs(py, Some(name.clone()));
        }
        // NOTE: Do we need a clone here?
        let mut specs = col_specs.get(&name).cloned();

        if columns.is_some() {
            if let Some(mut specs_) = specs {
                let columns_unwrap = columns.unwrap();

                let order_map: HashMap<_, _> = columns_unwrap
                    .iter()
                    .enumerate()
                    .map(|(i, name)| (name, i))
                    .collect();
                specs_.sort_by_key(|spec| order_map.get(&spec.name).copied());
                specs = Some(specs_)
                // new_specs = let mut Vec<ColumnSpec>;
            }
        }
        specs
    }
}
#[pymethods]
impl Scylla {
    #[new]
    #[pyo3(signature = (
        contact_points,
        *,
        username = None,
        password = None,
        keyspace = None,
        ssl_cert = None,
        connection_timeout = None,
        write_coalescing = None,
        pool_size_per_host = None,
        pool_size_per_shard = None,
        keepalive_interval = None,
        keepalive_timeout= None,
        tcp_keepalive_interval = None,
        tcp_nodelay = None,
        disallow_shard_aware_port = None,
        default_execution_profile = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn py_new(
        contact_points: Vec<String>,
        username: Option<String>,
        password: Option<String>,
        keyspace: Option<String>,
        ssl_cert: Option<String>,
        connection_timeout: Option<u64>,
        write_coalescing: Option<bool>,
        pool_size_per_host: Option<NonZeroUsize>,
        pool_size_per_shard: Option<NonZeroUsize>,
        keepalive_interval: Option<u64>,
        keepalive_timeout: Option<u64>,
        tcp_keepalive_interval: Option<u64>,
        tcp_nodelay: Option<bool>,
        disallow_shard_aware_port: Option<bool>,
        default_execution_profile: Option<ScyllaPyExecutionProfile>,
    ) -> Self {
        Scylla {
            contact_points,
            username,
            password,
            ssl_cert,
            keyspace,
            connection_timeout,
            write_coalescing,
            disallow_shard_aware_port,
            pool_size_per_host,
            pool_size_per_shard,
            keepalive_interval,
            keepalive_timeout,
            tcp_keepalive_interval,
            tcp_nodelay,
            default_execution_profile,
            scylla_session: Arc::new(tokio::sync::RwLock::new(None)),
            column_specs: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
    }

    /// Start the session.
    ///
    /// Here we create a new scylla session
    /// and save it in our structure.
    ///
    /// # Errors
    /// May return an error in several cases:
    /// * The session is already initialized;
    /// * Username passed without password and vice versa;
    /// * Cannot connect to the database.
    pub fn startup<'a>(&'a self, py: Python<'a>) -> ScyllaPyResult<&'a PyAny> {
        let contact_points = self.contact_points.clone();
        let username = self.username.clone();
        let password = self.password.clone();
        let mut ssl_context = None;
        if let Some(cert_data) = self.ssl_cert.clone() {
            let mut ssl_context_builder = SslContextBuilder::new(SslMethod::tls())?;
            let pem = X509::from_pem(cert_data.as_bytes())?;
            ssl_context_builder.set_certificate(&pem)?;
            ssl_context_builder.set_verify(SslVerifyMode::NONE);
            ssl_context = Some(ssl_context_builder.build());
        }
        let keyspace = self.keyspace.clone();
        let scylla_session = self.scylla_session.clone();
        let column_specs = self.column_specs.clone();
        let conn_timeout = self.connection_timeout;
        let write_coalescing = self.write_coalescing;
        let disallow_shard_aware_port = self.disallow_shard_aware_port;
        let pool_size_per_host = self.pool_size_per_host;
        let pool_size_per_shard = self.pool_size_per_shard;
        let keepalive_interval = self.keepalive_interval;
        let keepalive_timeout = self.keepalive_timeout;
        let tcp_keepalive_interval = self.tcp_keepalive_interval;
        let tcp_nodelay = self.tcp_nodelay;
        let default_execution_profile = self.default_execution_profile.clone();
        scyllapy_future(py, async move {
            if scylla_session.read().await.is_some() {
                return Err(ScyllaPyError::SessionError(
                    "Session already initialized.".into(),
                ));
            }
            let mut session_builder = scylla::SessionBuilder::new()
                .ssl_context(ssl_context)
                .known_nodes(contact_points);
            if let Some(write_coalescing) = write_coalescing {
                session_builder = session_builder.write_coalescing(write_coalescing);
            }
            if let Some(disallow) = disallow_shard_aware_port {
                session_builder = session_builder.disallow_shard_aware_port(disallow);
            }
            if let Some(pool_per_host) = pool_size_per_host {
                session_builder = session_builder
                    .pool_size(scylla::transport::session::PoolSize::PerHost(pool_per_host));
            } else if let Some(pool_size_per_shard) = pool_size_per_shard {
                session_builder = session_builder.pool_size(
                    scylla::transport::session::PoolSize::PerShard(pool_size_per_shard),
                );
            }
            if let Some(execution_prof) = default_execution_profile {
                session_builder =
                    session_builder.default_execution_profile_handle(execution_prof.into());
            }
            if let Some(inter) = keepalive_interval {
                session_builder = session_builder.keepalive_interval(Duration::from_secs(inter));
            }
            if let Some(timeout) = keepalive_timeout {
                session_builder = session_builder.keepalive_timeout(Duration::from_secs(timeout));
            }
            if let Some(inter) = tcp_keepalive_interval {
                session_builder =
                    session_builder.tcp_keepalive_interval(Duration::from_secs(inter));
            }
            if let Some(tcp_nodelay) = tcp_nodelay {
                session_builder = session_builder.tcp_nodelay(tcp_nodelay);
            }
            match (username, password) {
                (Some(user), Some(pass)) => session_builder = session_builder.user(user, pass),
                (None, None) => {}
                _ => {
                    return Err(ScyllaPyError::SessionError(
                        "Cannot use username without a password and vice versa.".into(),
                    ));
                }
            }
            if let Some(keyspace) = keyspace {
                session_builder = session_builder.use_keyspace(keyspace, true);
            }
            if let Some(connection_timeout) = conn_timeout {
                session_builder =
                    session_builder.connection_timeout(Duration::from_secs(connection_timeout));
            }
            let mut session_guard = scylla_session.write().await;
            *session_guard = Some(session_builder.build().await?);

            let Ok(session) = session_guard.as_ref().ok_or(ScyllaPyError::SessionError(
                "Session is not initialized.".into(),
            )) else {
                return Err(ScyllaPyError::SessionError("Session Init err".into()));
            };

            let Ok(result) = session.query("DESCRIBE tables", &[]).await else {
                return Err(ScyllaPyError::SessionError("No table found".into()));
            };

            let Ok(mut iter) = result.rows_typed::<(String, String, String)>() else {
                return Err(ScyllaPyError::SessionError("No table found".into()));
            };

            let mut new_specs = HashMap::new();
            while let Ok(Some((_keyspace, _type, table))) = iter.next().transpose() {
                let select_table = session
                    .query_iter(format!("SELECT * FROM {} LIMIT 1", table), &[])
                    .await;
                if select_table.is_ok() {
                    new_specs.insert(table, select_table.unwrap().get_column_specs().to_owned());
                }
                // session.execute
            }

            let mut column_guard = column_specs.write().await;
            *column_guard = new_specs;
            Ok(())
        })
    }

    /// Close current session, free resources.
    ///
    /// # Errors
    ///
    /// Returns error if session wasn't initialized before
    /// calling this method.
    pub fn shutdown<'a>(&'a self, py: Python<'a>) -> ScyllaPyResult<&'a PyAny> {
        let session = self.scylla_session.clone();
        scyllapy_future(py, async move {
            let mut guard = session.write().await;
            if guard.is_none() {
                return Err(ScyllaPyError::SessionError(
                    "The session is not initialized.".into(),
                ));
            }
            guard.take();
            Ok(())
        })
    }

    /// Execute a query.
    ///
    /// This function takes a query and other parameters
    /// for performing actual request to the database.
    ///
    /// It creates a python future and executes
    /// the query, using it's `scylla_session`.
    ///
    /// # Errors
    ///
    /// Can result in an error in any case, when something goes wrong.
    #[pyo3(signature = (query, params = None, table=None,columns=None, *, paged = 0))]
    pub fn execute<'a>(
        &'a self,
        py: Python<'a>,
        query: ExecuteInput,
        params: Option<&'a PyAny>,
        table: Option<String>,
        columns: Option<Vec<String>>,
        paged: i32,
    ) -> ScyllaPyResult<&'a PyAny> {
        let mut col_spec = None;
        // We need to prepare parameter we're going to use
        // in query.
        if let ExecuteInput::PreparedQuery(prepared) = &query {
            col_spec = Some(prepared.inner.get_variable_col_specs().to_owned());
        }
        if col_spec.is_none()
            && table.is_some()
            && (params.is_some() && params.unwrap().is_instance_of::<PyList>() || columns.is_some())
        {
            col_spec = self.get_col_specs(py, table.unwrap(), columns);
        }
        let query_params = parse_python_query_params(params, true, col_spec.as_deref())?;
        // We need this clone, to safely share the session between threads.
        let (query, prepared) = match query {
            ExecuteInput::Text(txt) => (Some(Query::new(txt)), None),
            ExecuteInput::Query(query) => (Some(Query::from(query)), None),
            ExecuteInput::PreparedQuery(prep) => (None, Some(PreparedStatement::from(prep))),
        };
        self.native_execute(py, query, prepared, query_params, paged)
    }

    /// Execute a batch statement.
    ///
    /// This function takes a batch and list of lists of params.
    #[pyo3(signature = (batch, params = None, table=None, columns=None, single_query=true))]
    pub fn batch<'a>(
        &'a self,
        py: Python<'a>,
        batch: BatchInput,
        params: Option<Vec<&'a PyAny>>,
        table: Option<String>,
        columns: Option<Vec<String>>,
        single_query: bool,
    ) -> ScyllaPyResult<&'a PyAny> {
        // We need to prepare parameter we're going to use
        // in query.
        // If parameters were passed, we parse python values,
        // to corresponding CQL values.
        let mut col_spec = None;

        let (batch, batch_params) = match batch {
            BatchInput::Batch(batch) => {
                let mut batch_params = Vec::new();
                if single_query {
                    if let BatchStatement::PreparedStatement(ps) = &batch.inner.statements[0] {
                        col_spec = Some(ps.get_variable_col_specs().to_owned());
                    };
                    if col_spec.is_none() && table.is_some() && columns.is_some() {
                        col_spec = self.get_col_specs(py, table.unwrap(), columns);
                    }
                }
                if let Some(passed_params) = params {
                    for query_params in passed_params {
                        batch_params.push(parse_python_query_params(
                            Some(query_params),
                            false,
                            col_spec.as_deref(),
                        )?);
                    }
                }
                (batch.into(), batch_params)
            }
            BatchInput::InlineBatch(inline) => inline.into(),
        };
        // We need this clone, to safely share the session between threads.
        let session_arc = self.scylla_session.clone();
        scyllapy_future(py, async move {
            let session_guard = session_arc.read().await;
            let session = session_guard.as_ref().ok_or(ScyllaPyError::SessionError(
                "Session is not initialized.".into(),
            ))?;
            let res = session.batch(&batch, batch_params).await?;
            Ok(ScyllaPyQueryResult::new(res))
        })
        .map_err(Into::into)
    }

    /// Prepare a query.
    ///
    /// This function takes a query to prepare
    /// and sends it to server.
    ///
    /// After preparation it returns a prepared
    /// query, that you can use later.
    pub fn prepare<'a>(
        &'a self,
        python: Python<'a>,
        query: PrepareInput,
    ) -> ScyllaPyResult<&'a PyAny> {
        let session_arc = self.scylla_session.clone();
        scyllapy_future(python, async move {
            let cql_query = Query::from(query);
            let session_guard = session_arc.read().await;
            let session = session_guard.as_ref().ok_or(ScyllaPyError::SessionError(
                "Session is not initialized.".into(),
            ))?;
            let prepared = session.prepare(cql_query).await?;
            Ok(ScyllaPyPreparedQuery::from(prepared))
        })
    }

    /// Set keyspace to all connections.
    ///
    /// # Errors
    /// May return an error, if
    /// sessions was not initialized.
    pub fn use_keyspace<'a>(
        &'a self,
        python: Python<'a>,
        keyspace: String,
    ) -> ScyllaPyResult<&'a PyAny> {
        let session_arc = self.scylla_session.clone();
        let column_specs = self.column_specs.clone();
        scyllapy_future(python, async move {
            let guard = session_arc.write().await;
            let session = guard.as_ref().ok_or(ScyllaPyError::SessionError(
                "Session is not initialized.".into(),
            ))?;
            session.use_keyspace(keyspace, true).await?;

            let Ok(result) = session.query("DESCRIBE tables", &[]).await else {
                return Err(ScyllaPyError::SessionError("No table found".into()));
            };

            let Ok(mut iter) = result.rows_typed::<(String, String, String)>() else {
                return Err(ScyllaPyError::SessionError("No table found".into()));
            };

            let mut new_specs = HashMap::new();
            while let Ok(Some((_keyspace, _type, table))) = iter.next().transpose() {
                let select_table = session
                    .query_iter(format!("SELECT * FROM {} LIMIT 1", table), &[])
                    .await;
                if select_table.is_ok() {
                    new_specs.insert(table, select_table.unwrap().get_column_specs().to_owned());
                }
                // session.execute
            }

            let mut column_guard = column_specs.write().await;
            *column_guard = new_specs;

            Ok(())
        })
    }

    /// Get current keyspace.
    ///
    /// # Errors
    /// May return an error, if
    /// sessions was not initialized.
    pub fn get_keyspace<'a>(&'a self, python: Python<'a>) -> ScyllaPyResult<&'a PyAny> {
        let session_arc = self.scylla_session.clone();
        scyllapy_future(python, async move {
            let guard = session_arc.write().await;
            let session = guard.as_ref().ok_or(ScyllaPyError::SessionError(
                "Session is not initialized.".into(),
            ))?;
            let keyspace = session.get_keyspace().map(|ks| (*ks).clone());
            Ok(keyspace)
        })
    }

    /// Refresh the column specs from the current keyspace.
    ///
    /// # Errors
    /// May return an error, if
    /// sessions was not initialized.
    #[pyo3(signature = (table=None))]
    pub fn refresh_column_specs<'a>(
        &'a self,
        python: Python<'a>,
        table: Option<String>,
    ) -> ScyllaPyResult<&'a PyAny> {
        let session_guard = self.scylla_session.clone();
        let column_specs = self.column_specs.clone();
        scyllapy_future(python, async move {
            let session_guard = session_guard.write().await;
            let Ok(session) = session_guard.as_ref().ok_or(ScyllaPyError::SessionError(
                "Session is not initialized.".into(),
            )) else {
                return Err(ScyllaPyError::SessionError("Session Init err".into()));
            };

            if table.is_none() {
                let Ok(result) = session.query("DESCRIBE tables", &[]).await else {
                    return Err(ScyllaPyError::SessionError("No table found".into()));
                };

                let Ok(mut iter) = result.rows_typed::<(String, String, String)>() else {
                    return Err(ScyllaPyError::SessionError("No table found".into()));
                };

                let mut new_specs = HashMap::new();
                while let Ok(Some((_keyspace, _type, table))) = iter.next().transpose() {
                    let select_table = session
                        .query_iter(format!("SELECT * FROM {} LIMIT 1", table), &[])
                        .await;
                    if select_table.is_ok() {
                        new_specs
                            .insert(table, select_table.unwrap().get_column_specs().to_owned());
                    }
                }
                let mut column_guard = column_specs.write().await;
                *column_guard = new_specs;
            } else {
                let select_table = session
                    .query_iter(
                        format!("SELECT * FROM {} LIMIT 1", table.as_ref().unwrap()),
                        &[],
                    )
                    .await;
                if select_table.is_ok() {
                    let mut column_guard = column_specs.write().await;
                    column_guard.insert(
                        table.unwrap(),
                        select_table.unwrap().get_column_specs().to_owned(),
                    );
                }
            }

            Ok(())
        })
    }
}
