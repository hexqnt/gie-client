use serde_json::Value;

use polars::prelude::{Column, NamedFrom, Series};

use crate::error::GieError;

use super::serde_ext::json_vec_to_string;
use super::types::{GieDate, format_date};

/// Shared column builder for AGSI and ALSI DataFrames.
#[derive(Debug)]
pub(crate) struct CommonFrameColumns<'a> {
    name: Vec<Option<&'a str>>,
    code: Vec<Option<&'a str>>,
    url: Vec<Option<&'a str>>,
    gas_day_start: Vec<Option<String>>,
    info_json: Vec<Option<String>>,
    children_json: Vec<Option<String>>,
}

impl<'a> CommonFrameColumns<'a> {
    /// Creates a column builder with preallocated capacity.
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            name: Vec::with_capacity(capacity),
            code: Vec::with_capacity(capacity),
            url: Vec::with_capacity(capacity),
            gas_day_start: Vec::with_capacity(capacity),
            info_json: Vec::with_capacity(capacity),
            children_json: Vec::with_capacity(capacity),
        }
    }

    /// Appends a row to the shared columns.
    pub(crate) fn push(
        &mut self,
        name: Option<&'a str>,
        code: Option<&'a str>,
        url: Option<&'a str>,
        gas_day_start: Option<GieDate>,
        info: Option<&[Value]>,
        children: Option<&[Value]>,
    ) -> Result<(), GieError> {
        self.name.push(name);
        self.code.push(code);
        self.url.push(url);
        self.gas_day_start.push(gas_day_start.map(format_date));
        self.info_json.push(json_vec_to_string(info)?);
        self.children_json.push(json_vec_to_string(children)?);
        Ok(())
    }

    /// Returns the number of appended rows.
    pub(crate) fn height(&self) -> usize {
        self.name.len()
    }

    /// Returns prefix and suffix columns for endpoint-specific DataFrame builders.
    pub(crate) fn into_polars_columns(self) -> (Vec<Column>, Vec<Column>) {
        let prefix = vec![
            Series::new("name".into(), self.name).into(),
            Series::new("code".into(), self.code).into(),
            Series::new("url".into(), self.url).into(),
            Series::new("gas_day_start".into(), self.gas_day_start).into(),
        ];
        let suffix = vec![
            Series::new("info_json".into(), self.info_json).into(),
            Series::new("children_json".into(), self.children_json).into(),
        ];
        (prefix, suffix)
    }
}
