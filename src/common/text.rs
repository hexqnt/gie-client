/// Trims surrounding whitespace while reusing the string buffer; converts blank values to `None`.
pub(crate) fn normalize_optional_text(mut value: String) -> Option<String> {
    let trimmed_start = value.trim_start();
    let leading_len = value.len() - trimmed_start.len();
    let trimmed = trimmed_start.trim_end();
    if trimmed.is_empty() {
        return None;
    }

    let trimmed_len = trimmed.len();
    value.truncate(leading_len + trimmed_len);
    if leading_len != 0 {
        value.drain(..leading_len);
    }
    Some(value)
}

#[cfg(test)]
mod tests {
    use super::normalize_optional_text;

    #[test]
    fn normalization_handles_unicode_and_preserves_inner_whitespace() {
        for input in ["", " \t\n", "\u{2003}\u{a0}"] {
            assert_eq!(normalize_optional_text(input.to_owned()), None);
        }
        for input in ["Газ LNG", "\u{2003}Газ LNG\u{a0}", " Газ LNG", "Газ LNG\n"] {
            assert_eq!(
                normalize_optional_text(input.to_owned()).as_deref(),
                Some("Газ LNG")
            );
        }
    }
}
