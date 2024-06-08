use std::any::type_name;


use super::error::{FsError, Result, TiFsResult};

pub fn deserialize_json<T: for<'sl> serde::Deserialize<'sl>>(bytes: &[u8]) -> TiFsResult<T> {
    serde_json::from_slice::<T>(bytes).map_err(|err| FsError::Serialize {
        target: type_name::<T>(),
        typ: "JSON",
        msg: err.to_string(),
    })
}

pub fn serialize_json<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|err| FsError::Serialize {
        target: type_name::<T>(),
        typ: "JSON",
        msg: err.to_string(),
    })
}
