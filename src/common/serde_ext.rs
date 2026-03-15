use serde::Deserialize;
use serde::de::{self, Deserializer};
use serde::ser::Serializer;
use serde_json::Value;

#[cfg(feature = "polars")]
use crate::error::GieError;

use super::types::{
    DatasetName, DatasetType, GieDate, RecordType, format_date, parse_dataset_name, parse_date,
    parse_record_type,
};

pub(crate) fn serialize_optional_date<S>(
    value: &Option<GieDate>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(date) => serializer.serialize_str(&format_date(*date)),
        None => serializer.serialize_none(),
    }
}

pub(crate) fn serialize_optional_dataset_type<S>(
    value: &Option<DatasetType>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(dataset_type) => serializer.serialize_str(dataset_type.as_str()),
        None => serializer.serialize_none(),
    }
}

pub(crate) fn deserialize_optional_dataset_name<'de, D>(
    deserializer: D,
) -> Result<Option<DatasetName>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;

    match value {
        None => Ok(None),
        Some(string) => {
            let trimmed = string.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(parse_dataset_name(trimmed)))
            }
        }
    }
}

pub(crate) fn deserialize_optional_record_type<'de, D>(
    deserializer: D,
) -> Result<Option<RecordType>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_optional_string(deserializer).map(|value| value.map(|raw| parse_record_type(&raw)))
}

pub(crate) fn deserialize_optional_date<'de, D>(
    deserializer: D,
) -> Result<Option<GieDate>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;

    match value {
        None => Ok(None),
        Some(string) => {
            let trimmed = string.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }

            parse_date(trimmed)
                .map(Some)
                .map_err(|message| de::Error::custom(format!("{message}, got {string:?}")))
        }
    }
}

pub(crate) fn deserialize_optional_f64<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<OptionalF64Input>::deserialize(deserializer)?;

    match value {
        None => Ok(None),
        Some(OptionalF64Input::Number(number)) => Ok(Some(number)),
        Some(OptionalF64Input::String(string)) => parse_optional_f64_string(&string)
            .map_err(|message| de::Error::custom(format!("{message}, got {string:?}"))),
        Some(OptionalF64Input::Object(object)) => parse_optional_f64_from_object(&object)
            .map_err(|message| de::Error::custom(format!("{message}, got {object:?}"))),
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum OptionalF64Input {
    Number(f64),
    String(String),
    Object(serde_json::Map<String, Value>),
}

fn parse_optional_f64_string(value: &str) -> Result<Option<f64>, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() || is_placeholder_numeric_string(trimmed) {
        return Ok(None);
    }

    trimmed
        .parse::<f64>()
        .map(Some)
        .map_err(|e| format!("invalid float string: {e}"))
}

fn is_placeholder_numeric_string(value: &str) -> bool {
    matches!(value, "-" | "--")
}

fn parse_optional_f64_value(value: &Value) -> Result<Option<f64>, String> {
    match value {
        Value::Null => Ok(None),
        Value::Number(number) => number
            .as_f64()
            .ok_or_else(|| "number is out of f64 range".to_string())
            .map(Some),
        Value::String(string) => parse_optional_f64_string(string),
        Value::Object(object) => parse_optional_f64_from_object(object),
        _ => Err("expected float/string/null/object with numeric fields".to_string()),
    }
}

fn parse_optional_f64_from_object(
    object: &serde_json::Map<String, Value>,
) -> Result<Option<f64>, String> {
    let preferred_keys = ["gwh", "lng", "value"];
    let mut saw_empty_or_null = false;
    let mut saw_parse_error = false;

    for key in preferred_keys {
        if let Some(value) = object.get(key) {
            match parse_optional_f64_value(value) {
                Ok(Some(parsed)) => return Ok(Some(parsed)),
                Ok(None) => saw_empty_or_null = true,
                Err(_) => saw_parse_error = true,
            }
        }
    }

    for (key, value) in object {
        if preferred_keys.contains(&key.as_str()) {
            continue;
        }
        match parse_optional_f64_value(value) {
            Ok(Some(parsed)) => return Ok(Some(parsed)),
            Ok(None) => saw_empty_or_null = true,
            Err(_) => saw_parse_error = true,
        }
    }

    if saw_empty_or_null {
        return Ok(None);
    }
    if saw_parse_error {
        return Err("object contains no parseable numeric fields".to_string());
    }

    Err("object is empty".to_string())
}

pub(crate) fn deserialize_optional_string<'de, D>(
    deserializer: D,
) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<OptionalStringInput>::deserialize(deserializer)?;

    match value {
        None => Ok(None),
        Some(OptionalStringInput::String(string)) => {
            let trimmed = string.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(string))
            }
        }
        Some(OptionalStringInput::Number(number)) => Ok(Some(number.to_string())),
        Some(OptionalStringInput::Bool(flag)) => Ok(Some(flag.to_string())),
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum OptionalStringInput {
    String(String),
    Number(serde_json::Number),
    Bool(bool),
}

#[cfg(feature = "polars")]
pub(crate) fn json_vec_to_string(
    value: Option<&[serde_json::Value]>,
) -> Result<Option<String>, GieError> {
    value
        .map(serde_json::to_string)
        .transpose()
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use super::*;

    #[derive(Debug, Deserialize)]
    struct FloatProbe {
        #[serde(deserialize_with = "deserialize_optional_f64")]
        value: Option<f64>,
    }

    #[derive(Debug, Deserialize)]
    struct StringProbe {
        #[serde(deserialize_with = "deserialize_optional_string")]
        value: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    struct DateProbe {
        #[serde(deserialize_with = "deserialize_optional_date")]
        value: Option<GieDate>,
    }

    #[derive(Debug, Serialize)]
    struct DateSerializeProbe {
        #[serde(serialize_with = "serialize_optional_date")]
        value: Option<GieDate>,
    }

    #[derive(Debug, Deserialize)]
    struct DatasetNameProbe {
        #[serde(deserialize_with = "deserialize_optional_dataset_name")]
        value: Option<DatasetName>,
    }

    #[derive(Debug, Deserialize)]
    struct RecordTypeProbe {
        #[serde(deserialize_with = "deserialize_optional_record_type")]
        value: Option<RecordType>,
    }

    fn test_date(value: &str) -> GieDate {
        parse_date(value).unwrap()
    }

    #[test]
    fn record_type_deserializer_accepts_values_and_blank_as_none() {
        let as_country: RecordTypeProbe = serde_json::from_str(r#"{"value": "country"}"#).unwrap();
        let as_unknown: RecordTypeProbe = serde_json::from_str(r#"{"value": "pipeline"}"#).unwrap();
        let as_blank: RecordTypeProbe = serde_json::from_str(r#"{"value": ""}"#).unwrap();
        let as_null: RecordTypeProbe = serde_json::from_str(r#"{"value": null}"#).unwrap();

        assert_eq!(as_country.value, Some(RecordType::Country));
        assert_eq!(
            as_unknown.value,
            Some(RecordType::Unknown("pipeline".to_string()))
        );
        assert_eq!(as_blank.value, None);
        assert_eq!(as_null.value, None);
    }

    #[test]
    fn dataset_name_deserializer_accepts_values_and_blank_as_none() {
        let as_storage: DatasetNameProbe = serde_json::from_str(r#"{"value": "storage"}"#).unwrap();
        let as_unknown: DatasetNameProbe =
            serde_json::from_str(r#"{"value": "storage ERROR"}"#).unwrap();
        let as_blank: DatasetNameProbe = serde_json::from_str(r#"{"value": ""}"#).unwrap();
        let as_null: DatasetNameProbe = serde_json::from_str(r#"{"value": null}"#).unwrap();

        assert_eq!(as_storage.value, Some(DatasetName::Storage));
        assert_eq!(
            as_unknown.value,
            Some(DatasetName::Unknown("storage ERROR".to_string()))
        );
        assert_eq!(as_blank.value, None);
        assert_eq!(as_null.value, None);
    }

    #[test]
    fn date_deserializer_accepts_ymd_or_empty() {
        let as_string: DateProbe = serde_json::from_str(r#"{"value": "2026-03-10"}"#).unwrap();
        let as_empty: DateProbe = serde_json::from_str(r#"{"value": ""}"#).unwrap();
        let as_null: DateProbe = serde_json::from_str(r#"{"value": null}"#).unwrap();

        assert_eq!(as_string.value, Some(test_date("2026-03-10")));
        assert_eq!(as_empty.value, None);
        assert_eq!(as_null.value, None);
    }

    #[test]
    fn date_deserializer_rejects_invalid_ymd() {
        assert!(serde_json::from_str::<DateProbe>(r#"{"value": "2026-13-10"}"#).is_err());
    }

    #[test]
    fn optional_date_serializer_outputs_ymd() {
        let payload = DateSerializeProbe {
            value: Some(test_date("2026-03-10")),
        };
        let json = serde_json::to_string(&payload).unwrap();
        assert_eq!(json, r#"{"value":"2026-03-10"}"#);
    }

    #[test]
    fn float_deserializer_accepts_number_and_string() {
        let as_number: FloatProbe = serde_json::from_str(r#"{"value": 1.5}"#).unwrap();
        let as_string: FloatProbe = serde_json::from_str(r#"{"value": "2.5"}"#).unwrap();
        let as_empty: FloatProbe = serde_json::from_str(r#"{"value": ""}"#).unwrap();
        let as_dash: FloatProbe = serde_json::from_str(r#"{"value": "-"}"#).unwrap();
        let as_double_dash: FloatProbe = serde_json::from_str(r#"{"value": "--"}"#).unwrap();

        assert_eq!(as_number.value, Some(1.5));
        assert_eq!(as_string.value, Some(2.5));
        assert_eq!(as_empty.value, None);
        assert_eq!(as_dash.value, None);
        assert_eq!(as_double_dash.value, None);
    }

    #[test]
    fn float_deserializer_accepts_object_with_units() {
        let as_object: FloatProbe =
            serde_json::from_str(r#"{"value": {"lng": "779.06", "gwh": "5222.32"}}"#).unwrap();
        let as_lng_only: FloatProbe =
            serde_json::from_str(r#"{"value": {"lng": "779.06"}}"#).unwrap();
        let as_placeholders: FloatProbe =
            serde_json::from_str(r#"{"value": {"lng": "-", "gwh": "-"}}"#).unwrap();

        assert_eq!(as_object.value, Some(5222.32));
        assert_eq!(as_lng_only.value, Some(779.06));
        assert_eq!(as_placeholders.value, None);
    }

    #[test]
    fn string_deserializer_accepts_number_bool_and_string() {
        let as_string: StringProbe = serde_json::from_str(r#"{"value": "ok"}"#).unwrap();
        let as_number: StringProbe = serde_json::from_str(r#"{"value": 42}"#).unwrap();
        let as_bool: StringProbe = serde_json::from_str(r#"{"value": true}"#).unwrap();
        let as_empty: StringProbe = serde_json::from_str(r#"{"value": ""}"#).unwrap();

        assert_eq!(as_string.value.as_deref(), Some("ok"));
        assert_eq!(as_number.value.as_deref(), Some("42"));
        assert_eq!(as_bool.value.as_deref(), Some("true"));
        assert_eq!(as_empty.value, None);
    }
}
