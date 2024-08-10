use pyo3::{
    pyclass, pymethods,
    types::{PyDict, PyList},
    Py, PyAny, PyRefMut, Python,
};
use scylla::query::Query;

use super::utils::{pretty_build, IfCluase, Timeout};
use crate::{
    batches::ScyllaPyInlineBatch,
    exceptions::rust_err::{ScyllaPyError, ScyllaPyResult},
    queries::ScyllaPyRequestParams,
    scylla_cls::Scylla,
    utils::parse_python_query_params,
};


#[pyclass]
#[derive(Clone, Debug, Default)]
pub struct Delete {
    table_: String,
    columns: Option<Vec<String>>,
    timeout_: Option<Timeout>,
    timestamp_: Option<u64>,
    if_clause_: Option<IfCluase>,
    where_clauses_: Vec<String>,
    raw_values_: Vec<Py<PyAny>>,
    request_params_: ScyllaPyRequestParams,
}

impl Delete {
    fn build_query(&self) -> ScyllaPyResult<String> {
        if self.where_clauses_.is_empty() {
            return Err(ScyllaPyError::QueryBuilderError(
                "At least one where clause should be specified.",
            ));
        }
        let columns = self
            .columns
            .as_ref()
            .map_or(String::new(), |cols| cols.join(", "));
        let params = [
            self.timestamp_
                .map(|timestamp| format!("TIMESTAMP {timestamp}")),
            self.timeout_.as_ref().map(|timeout| match timeout {
                Timeout::Int(int) => format!("TIMEOUT {int}"),
                Timeout::Str(string) => format!("TIMEOUT {string}"),
            }),
        ];
        let prepared_params = params
            .iter()
            .map(|item| item.as_ref().map_or("", String::as_str))
            .filter(|item| !item.is_empty())
            .collect::<Vec<_>>();
        let usings = if prepared_params.is_empty() {
            String::new()
        } else {
            format!("USING {}", prepared_params.join(" AND "))
        };
        let where_clause = format!("WHERE {}", self.where_clauses_.join(" AND "));
        let if_conditions = self
            .if_clause_
            .as_ref()
            .map_or(String::default(), |cond| match cond {
                IfCluase::Exists => String::from("IF EXISTS"),
                IfCluase::Condition { clauses, values: _ } => {
                    format!("IF {}", clauses.join(" AND "))
                }
            });
        Ok(pretty_build([
            "DELETE",
            columns.as_str(),
            "FROM",
            self.table_.as_str(),
            usings.as_str(),
            where_clause.as_str(),
            if_conditions.as_str(),
        ]))
    }
}

#[pymethods]
impl Delete {
    #[new]
    #[must_use]
    pub fn py_new(table: String) -> Self {
        Self {
            table_: table,
            ..Default::default()
        }
    }

    #[must_use]
    #[pyo3(signature = (*cols))]
    pub fn cols(mut slf: PyRefMut<'_, Self>, cols: Vec<String>) -> PyRefMut<'_, Self> {
        slf.columns = Some(cols);
        slf
    }

    /// Add where clause.
    ///
    /// This function adds where with values.
    ///
    /// # Errors
    ///
    /// Can return an error, if values
    /// cannot be parsed.
    #[pyo3(signature = (clause, values = None))]
    pub fn r#where<'a>(
        mut slf: PyRefMut<'a, Self>,
        clause: String,
        values: Option<Vec<&'a PyAny>>,
    ) -> ScyllaPyResult<PyRefMut<'a, Self>> {
        slf.where_clauses_.push(clause);
        if let Some(vals) = values {
            for value in vals {
                slf.raw_values_.push(value.into());
            }
        }
        Ok(slf)
    }

    #[must_use]
    pub fn timeout(mut slf: PyRefMut<'_, Self>, timeout: Timeout) -> PyRefMut<'_, Self> {
        slf.timeout_ = Some(timeout);
        slf
    }

    #[must_use]
    pub fn timestamp(mut slf: PyRefMut<'_, Self>, timestamp: u64) -> PyRefMut<'_, Self> {
        slf.timestamp_ = Some(timestamp);
        slf
    }

    #[must_use]
    pub fn if_exists(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.if_clause_ = Some(IfCluase::Exists);
        slf
    }

    /// Add if clause.
    ///
    /// # Errors
    ///
    /// May return an error, if values
    /// cannot be converted to rust types.
    #[pyo3(signature = (clause, values = None))]
    pub fn if_<'a>(
        mut slf: PyRefMut<'a, Self>,
        clause: String,
        values: Option<Vec<&'a PyAny>>,
    ) -> ScyllaPyResult<PyRefMut<'a, Self>> {
        let values_clause: Vec<Py<PyAny>> = match values {
            Some(val) => val.iter().map(|x| x.clone().into()).collect(),
            None => vec![],
        };
        match slf.if_clause_.as_mut() {
            Some(IfCluase::Condition { clauses, values }) => {
                clauses.push(clause);
                values.extend(values_clause);
            }
            None | Some(IfCluase::Exists) => {
                slf.if_clause_ = Some(IfCluase::Condition {
                    clauses: vec![clause],
                    values: values_clause,
                });
            }
        }
        Ok(slf)
    }

    /// Add parameters to the request.
    ///
    /// These parameters are used by scylla.
    ///
    /// # Errors
    ///
    /// May return an error, if request parameters
    /// cannot be built.
    #[pyo3(signature = (**params))]
    pub fn request_params<'a>(
        mut slf: PyRefMut<'a, Self>,
        params: Option<&'a PyDict>,
    ) -> ScyllaPyResult<PyRefMut<'a, Self>> {
        slf.request_params_ = ScyllaPyRequestParams::from_dict(params)?;
        Ok(slf)
    }

    /// Execute a query.
    ///
    /// # Errors
    ///
    /// May return an error, if something goes wrong
    /// during query building
    /// or during query execution.
    pub fn execute<'a>(&'a self, py: Python<'a>, scylla: &'a Scylla) -> ScyllaPyResult<&'a PyAny> {
        let mut query = Query::new(self.build_query()?);
        self.request_params_.apply_to_query(&mut query);

        let values = if let Some(if_clause) = &self.if_clause_ {
            if_clause.extend_values(self.raw_values_.clone())
        } else {
            self.raw_values_.clone()
        };
        // Dirty but necessary to use a .spawn(async move {})
        let scylla_clone = scylla.clone();
        let query_clone = query.clone();

        let runtime = pyo3_asyncio::tokio::get_runtime();
        let prepared = runtime
            .block_on(async move {
                runtime.spawn(
                    async move  {scylla_clone.prepare_query(query_clone).await}
                ).await})
            .unwrap()?;

        let col_spec = Some(prepared.get_variable_col_specs().to_owned());
        let params = PyList::new(py, values);
        let serialized = parse_python_query_params(Some(params), true, col_spec.as_deref())?;
        scylla.native_execute(py, None::<Query>, Some(prepared), serialized, 0)
    }

    /// Add to batch
    ///
    /// Adds current query to batch.
    ///
    /// # Errors
    ///
    /// May result into error if query cannot be build.
    /// Or values cannot be passed to batch.
    pub fn add_to_batch<'a>(
        &'a self,
        py: Python<'a>,
        scylla: &'a Scylla,
        batch: &mut ScyllaPyInlineBatch,
    ) -> ScyllaPyResult<()> {
        let mut query = Query::new(self.build_query()?);
        self.request_params_.apply_to_query(&mut query);

        let values = if let Some(if_clause) = &self.if_clause_ {
            if_clause.extend_values(self.raw_values_.clone())
        } else {
            self.raw_values_.clone()
        };
        // Dirty but necessary to use a .spawn(async move {})
        let scylla_clone = scylla.clone();
        let query_clone = query.clone();

        let runtime = pyo3_asyncio::tokio::get_runtime();
        let prepared = runtime
            .block_on(async move {
                runtime.spawn(
                    async move  {scylla_clone.prepare_query(query_clone).await}
                ).await})
            .unwrap()?;

        let col_spec = Some(prepared.get_variable_col_specs().to_owned());
        let params = PyList::new(py, values.clone());
        let serialized = parse_python_query_params(Some(params), true, col_spec.as_deref())?;

        batch.add_query_inner(prepared, serialized);
        Ok(())
    }

    #[must_use]
    pub fn __repr__(&self) -> String {
        format!("{self:?}")
    }

    /// Convert query to string.
    ///
    /// # Errors
    ///
    /// May return an error if something
    /// goes wrong during query building.
    pub fn __str__(&self) -> ScyllaPyResult<String> {
        self.build_query()
    }

    #[must_use]
    pub fn __copy__(&self) -> Self {
        self.clone()
    }

    #[must_use]
    pub fn __deepcopy__(&self, _memo: &PyDict) -> Self {
        self.clone()
    }
}
