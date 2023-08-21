use std::{future::Future, str::FromStr};

use pyo3::{
    types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PySet, PyString, PyTuple},
    IntoPy, Py, PyAny, PyObject, Python, ToPyObject,
};
use scylla::{
    _macro_internal::{CqlValue, Value},
    frame::response::result::ColumnType,
};

use std::net::IpAddr;

/// Small function to integrate anyhow result
/// and `pyo3_asyncio`.
///
/// It's almost the same as `future_into_py`,
/// but it expects future to return anyhow result, rather
/// than `PyResult` from `pyo3`. It's useful for using `?` operators all over the place.
///
/// # Errors
///
/// If result of a future was unsuccessful, it propagates the error.
pub fn anyhow_py_future<F, T>(py: Python<'_>, fut: F) -> anyhow::Result<&PyAny>
where
    F: Future<Output = anyhow::Result<T>> + Send + 'static,
    T: IntoPy<PyObject>,
{
    let res = pyo3_asyncio::tokio::future_into_py(py, async { fut.await.map_err(Into::into) })
        .map(Into::into)?;
    Ok(res)
}

pub enum PyToValue {
    String(String),
    BigInt(i64),
    Int(i32),
    SmallInt(i16),
    Bool(bool),
    Double(f64),
    Float(f32),
    Bytes(Vec<u8>),
    Uuid(uuid::Uuid),
    Inet(IpAddr),
    List(Vec<PyToValue>),
}

impl Value for PyToValue {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), scylla::_macro_internal::ValueTooBig> {
        match self {
            PyToValue::String(s) => s.serialize(buf),
            PyToValue::BigInt(i) => i.serialize(buf),
            PyToValue::Int(i) => i.serialize(buf),
            PyToValue::SmallInt(i) => i.serialize(buf),
            PyToValue::Bool(b) => b.serialize(buf),
            PyToValue::Double(d) => d.serialize(buf),
            PyToValue::Float(f) => f.serialize(buf),
            PyToValue::Bytes(b) => b.serialize(buf),
            PyToValue::Uuid(u) => u.serialize(buf),
            PyToValue::Inet(i) => i.serialize(buf),
            PyToValue::List(l) => l.serialize(buf),
        }
    }
}

/// Convert Python type to CQL parameter value.
///
/// It converts python object to another type,
/// which can be serialized as Value that can
/// be bound to `Query`.
///
/// # Errors
///
/// May raise an error, if
/// value cannot be converted or unnown type was passed.
pub fn py_to_value(item: &PyAny) -> anyhow::Result<PyToValue> {
    if item.is_instance_of::<PyString>() {
        Ok(PyToValue::String(item.extract::<String>()?))
    } else if item.is_instance_of::<PyInt>() {
        Ok(PyToValue::BigInt(item.extract::<i64>()?))
    } else if item.is_instance_of::<PyBool>() {
        Ok(PyToValue::Bool(item.extract::<bool>()?))
    } else if item.is_instance_of::<PyFloat>() {
        Ok(PyToValue::Double(item.extract::<f64>()?))
    } else if item.is_instance_of::<PyBytes>() {
        Ok(PyToValue::Bytes(item.extract::<Vec<u8>>()?))
    } else if item.get_type().name()? == "UUID" {
        Ok(PyToValue::Uuid(uuid::Uuid::parse_str(
            item.str()?.extract::<&str>()?,
        )?))
    } else if item.get_type().name()? == "IPv4Address" || item.get_type().name()? == "IPv6Address" {
        Ok(PyToValue::Inet(IpAddr::from_str(
            item.str()?.extract::<&str>()?,
        )?))
    } else if item.is_instance_of::<PyList>()
        || item.is_exact_instance_of::<PyTuple>()
        || item.is_exact_instance_of::<PySet>()
    {
        let mut items = Vec::new();
        for inner in item.iter()? {
            items.push(py_to_value(inner?)?);
        }
        Ok(PyToValue::List(items))
    } else {
        let type_name = item.get_type().name()?;
        Err(anyhow::anyhow!(
            "Unsupported type for parameter binding: {type_name:?}"
        ))
    }
}

/// Convert CQL type from database to Python.
///
/// This function takes a CQL value from database
/// response and converts it to some python type,
/// loading it in interpreter, so it can be referenced
/// from python code.
///
/// `cql_type` is the type that database sent to us.
/// Used to parse the value with appropriate parser.
///
///
/// # Errors
///
/// This function can throw an error, if it was unable
/// to parse thr type, or if type is not supported.
#[allow(clippy::too_many_lines)]
pub fn cql_to_py<'a>(
    py: Python<'a>,
    cql_type: &'a ColumnType,
    cql_value: Option<&CqlValue>,
) -> anyhow::Result<&'a PyAny> {
    let Some(unwrapped_value) = cql_value else{
        return Ok(py.None().into_ref(py));
    };
    match cql_type {
        ColumnType::Ascii => unwrapped_value
            .as_ascii()
            .ok_or(anyhow::anyhow!("Cannot parse"))
            .map(|val| PyString::new(py, val).as_ref()),
        ColumnType::Boolean => unwrapped_value
            .as_boolean()
            .ok_or(anyhow::anyhow!("Cannot parse"))
            .map(|val| PyBool::new(py, val).as_ref()),
        ColumnType::Blob => unwrapped_value
            .as_blob()
            .ok_or(anyhow::anyhow!("Cannot parse"))
            .map(|val| PyBytes::new(py, val.as_ref()).as_ref()),
        ColumnType::Double => unwrapped_value
            .as_double()
            .ok_or(anyhow::anyhow!("Cannot parse"))
            .map(|val| val.to_object(py).into_ref(py)),
        ColumnType::Float => unwrapped_value
            .as_double()
            .ok_or(anyhow::anyhow!("Cannot parse"))
            .map(|val| val.to_object(py).into_ref(py)),
        ColumnType::Int => unwrapped_value
            .as_int()
            .ok_or(anyhow::anyhow!("Cannot parse"))
            .map(|val| val.to_object(py).into_ref(py)),
        ColumnType::BigInt => unwrapped_value
            .as_bigint()
            .ok_or(anyhow::anyhow!("Cannot parse"))
            .map(|val| val.to_object(py).into_ref(py)),
        ColumnType::Text => unwrapped_value
            .as_text()
            .ok_or(anyhow::anyhow!("Cannot parse"))
            .map(|val| val.to_object(py).into_ref(py)),
        ColumnType::List(column_type) => {
            let items = unwrapped_value
                .as_list()
                .ok_or(anyhow::anyhow!("Cannot parse"))?
                .iter()
                .map(|val| cql_to_py(py, column_type.as_ref(), Some(val)))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(items.to_object(py).into_ref(py))
        }
        ColumnType::Map(key_type, val_type) => {
            let map_values = unwrapped_value
                .as_map()
                .ok_or(anyhow::anyhow!("Cannot parse"))?
                .iter()
                .map(|(key, val)| -> anyhow::Result<(&'a PyAny, &'a PyAny)> {
                    Ok((
                        cql_to_py(py, key_type, Some(key))?,
                        cql_to_py(py, val_type, Some(val))?,
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?;

            let res_map = PyDict::new(py);
            for (key, value) in map_values {
                res_map.set_item(key, value)?;
            }
            Ok(res_map)
        }
        ColumnType::Set(column_type) => {
            let items = unwrapped_value
                .as_set()
                .ok_or(anyhow::anyhow!("Cannot parse"))?
                .iter()
                .map(|val| cql_to_py(py, column_type.as_ref(), Some(val)))
                .collect::<Result<Vec<_>, _>>()?;
            let res_set = PySet::new(py, items)?;
            Ok(res_set)
        }
        ColumnType::SmallInt => unwrapped_value
            .as_smallint()
            .ok_or(anyhow::anyhow!("Cannot parse"))
            .map(|val| val.to_object(py).into_ref(py)),
        ColumnType::TinyInt => unwrapped_value
            .as_tinyint()
            .ok_or(anyhow::anyhow!("Cannot parse"))
            .map(|val| val.to_object(py).into_ref(py)),
        ColumnType::Uuid => {
            let uuid_str = unwrapped_value
                .as_uuid()
                .ok_or(anyhow::anyhow!(""))?
                .simple()
                .to_string();
            Ok(py.import("uuid")?.getattr("UUID")?.call1((uuid_str,))?)
        }
        ColumnType::Timeuuid => {
            let uuid_str = unwrapped_value
                .as_timeuuid()
                .ok_or(anyhow::anyhow!("Cannot parse timeuuid"))?
                .as_simple()
                .to_string();
            Ok(py.import("uuid")?.getattr("UUID")?.call1((uuid_str,))?)
        }
        ColumnType::Duration => {
            // We loose some perscision on converting it to
            // python datetime, because in scylla,
            // durations is stored in nanoseconds.
            // But that's ok, because we assume that
            // all values were inserted using
            // same driver. Will fix it on demand.
            let duration = unwrapped_value
                .as_duration()
                .ok_or(anyhow::anyhow!("Cannot parse duration"))?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("microseconds", duration.num_microseconds())?;
            Ok(py
                .import("datetime")?
                .getattr("timedelta")?
                .call((), Some(kwargs))?)
        }
        ColumnType::Timestamp => {
            // Timestamp - num of milliseconds since unix epoch.
            Ok(unwrapped_value
                .as_duration()
                .ok_or(anyhow::anyhow!("Cannot parse timestamp"))?
                .num_milliseconds()
                .to_object(py)
                .into_ref(py))
        }
        ColumnType::Inet => Ok(unwrapped_value
            .as_inet()
            .ok_or(anyhow::anyhow!("Cannot parse inet addres"))?
            .to_object(py)
            .into_ref(py)),
        ColumnType::Date => {
            let formatted_date = unwrapped_value
                .as_date()
                .ok_or(anyhow::anyhow!("Cannot parse date"))?
                .format("%Y-%m-%d")
                .to_string();
            Ok(py
                .import("datetime")?
                .getattr("date")?
                .call_method1("fromisoformat", (formatted_date,))?)
        }
        ColumnType::Tuple(types) => {
            if let CqlValue::Tuple(data) = unwrapped_value {
                let mut dumped_elemets = Vec::new();
                for (col_type, col_val) in types.iter().zip(data) {
                    dumped_elemets.push(cql_to_py(py, col_type, col_val.as_ref())?);
                }
                Ok(PyTuple::new(py, dumped_elemets))
            } else {
                Err(anyhow::anyhow!("Cannot parse as tuple."))
            }
        }
        ColumnType::Time => Err(anyhow::anyhow!("Time is not yet supported.")),
        ColumnType::Counter => Err(anyhow::anyhow!("Counter is not yet supported.")),
        ColumnType::Custom(_) => Err(anyhow::anyhow!("Custom types are not yet supported.")),
        ColumnType::Varint => Err(anyhow::anyhow!("Variant is not yet supported.")),
        ColumnType::Decimal => Err(anyhow::anyhow!("Decimals are not yet supported.")),
        ColumnType::UserDefinedType {
            type_name: _,
            keyspace: _,
            field_types: _,
        } => Err(anyhow::anyhow!("UDT is not yet supported.")),
    }
}

/// Map rows, using some python callable.
///
/// This function casts every row to dictionary
/// and passes it as key-word arguments to the
/// `as_class` function. Returned values are
/// then written in a new vector of mapped rows.
///
/// # Errors
/// May result in an error if
/// * `rows` object cannot be casted to `PyList`;
/// * At least one row cannot be casted to dict.
pub fn map_rows<'a>(
    py: Python<'a>,
    rows: &'a Py<PyAny>,
    as_class: &'a Py<PyAny>,
) -> anyhow::Result<Vec<Py<PyAny>>> {
    let mapped_rows = rows
        .downcast::<PyList>(py)
        .map_err(|_| anyhow::anyhow!("Cannot downcast rows to list."))?
        .iter()
        .map(|obj| {
            as_class.call(
                py,
                (),
                Some(
                    obj.downcast::<PyDict>()
                        .map_err(|_| anyhow::anyhow!("Cannot preapre kwargs for mapping."))?,
                ),
            )
        })
        .collect::<anyhow::Result<Vec<_>, _>>()?;
    Ok(mapped_rows)
}
