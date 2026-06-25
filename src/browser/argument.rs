use std::collections::HashMap;
use std::fmt;

/// Chrome switches whose value is a comma-separated LIST rather than a single
/// scalar. For these, a default and a user value should be *unioned* (both
/// contribute); every other switch is scalar, where a user value *replaces* the
/// default. Used by [`ArgsBuilder::arg_defaults`].
const MERGEABLE_LIST_SWITCHES: [&str; 3] =
    ["enable-features", "disable-features", "enable-blink-features"];

pub struct ArgsBuilder(HashMap<String, Vec<String>>);

impl ArgsBuilder {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn has(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }

    pub fn arg<T: Into<Arg>>(&mut self, arg: T) -> &mut Self {
        let arg = arg.into();
        if let Some(values) = self.0.get_mut(&arg.key) {
            values.extend(arg.values);
        } else {
            self.0.insert(arg.key, arg.values);
        }
        self
    }

    pub fn args<T: Into<Arg>>(&mut self, args: impl IntoIterator<Item = T>) -> &mut Self {
        for arg in args {
            self.arg(arg);
        }
        self
    }

    /// Merge in *default* arguments without clobbering explicit user values.
    ///
    /// Two cases, because Chrome switches are not all alike:
    ///
    /// * **Scalar switches** (e.g. `lang`, `force-color-profile`,
    ///   `password-store`): a default fills the key only if the user did NOT
    ///   set it. Unlike [`ArgsBuilder::arg`] (which *extends* on collision),
    ///   this lets an explicit `lang=ko-KR` REPLACE the built-in `lang=en_US`
    ///   instead of merging into the nonsensical `--lang=en_US,ko-KR` (which
    ///   Chrome ignores). Mirrors the `has("remote-debugging-port")` gate in
    ///   [`super::config::BrowserConfig::launch`].
    /// * **Comma-list switches** ([`MERGEABLE_LIST_SWITCHES`], e.g.
    ///   `disable-features`): the default's values are unioned with any
    ///   user-set ones, so a caller adding a feature flag still keeps
    ///   chromiumoxide's built-in `disable-features=TranslateUI` etc.
    pub fn arg_defaults<T: Into<Arg>>(&mut self, args: impl IntoIterator<Item = T>) -> &mut Self {
        for arg in args {
            let arg = arg.into();
            if MERGEABLE_LIST_SWITCHES.contains(&arg.key.as_str()) {
                self.0.entry(arg.key).or_default().extend(arg.values);
            } else {
                self.0.entry(arg.key).or_insert(arg.values);
            }
        }
        self
    }

    pub fn into_iter(self) -> impl Iterator<Item = String> {
        self.0.into_iter().map(|(key, values)| {
            if values.is_empty() {
                format!("--{}", key)
            } else {
                format!("--{}={}", key, values.join(","))
            }
        })
    }
}

#[derive(Debug, Clone)]
pub struct Arg {
    key: String,
    values: Vec<String>,
}

impl Arg {
    pub fn key(key: impl AsRef<str>) -> Self {
        Self {
            key: key.as_ref().to_string(),
            values: Vec::new(),
        }
    }

    pub fn value(key: impl AsRef<str>, value: impl fmt::Display) -> Self {
        Self {
            key: key.as_ref().to_string(),
            values: vec![value.to_string()],
        }
    }

    pub fn values(
        key: impl AsRef<str>,
        values: impl IntoIterator<Item = impl fmt::Display>,
    ) -> Self {
        Self {
            key: key.as_ref().to_string(),
            values: values.into_iter().map(|v| v.to_string()).collect(),
        }
    }
}

impl From<(&str, &str)> for Arg {
    fn from((key, value): (&str, &str)) -> Self {
        Self {
            key: key.to_string(),
            values: vec![value.to_string()],
        }
    }
}

impl From<(&str, &[&str])> for Arg {
    fn from((key, values): (&str, &[&str])) -> Self {
        Self {
            key: key.to_string(),
            values: values.iter().map(|v| v.to_string()).collect(),
        }
    }
}

impl From<&str> for Arg {
    fn from(value: &str) -> Self {
        Self {
            key: value.to_string(),
            values: Vec::new(),
        }
    }
}

impl From<String> for Arg {
    fn from(value: String) -> Self {
        Self {
            key: value,
            values: Vec::new(),
        }
    }
}

impl From<ArgConst> for Arg {
    fn from(arg: ArgConst) -> Self {
        Self {
            key: arg.key.to_string(),
            values: arg.values.iter().map(|v| v.to_string()).collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ArgConst {
    key: &'static str,
    values: &'static [&'static str],
}

impl ArgConst {
    pub const fn key(key: &'static str) -> Self {
        Self { key, values: &[] }
    }

    pub const fn values(key: &'static str, values: &'static [&'static str]) -> Self {
        Self { key, values }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rendered(builder: ArgsBuilder) -> Vec<String> {
        let mut args: Vec<String> = builder.into_iter().collect();
        args.sort();
        args
    }

    #[test]
    fn arg_extends_values_on_key_collision() {
        // `arg`/`args` keep upstream's merge semantics: same key accumulates a
        // comma-joined value list (correct for list switches like features).
        let mut b = ArgsBuilder::new();
        b.arg(Arg::value("disable-features", "TranslateUI"))
            .arg(Arg::value("disable-features", "HttpsUpgrades"));
        assert_eq!(rendered(b), vec!["--disable-features=TranslateUI,HttpsUpgrades"]);
    }

    #[test]
    fn arg_defaults_yields_to_an_already_set_key() {
        // A user-set single-valued arg must REPLACE the matching default, not
        // merge with it — the `--lang=en_US,ko-KR` bug this method fixes.
        let mut b = ArgsBuilder::new();
        b.arg(Arg::value("lang", "ko-KR"))
            .arg_defaults([Arg::value("lang", "en_US"), Arg::key("disable-sync")]);
        assert_eq!(
            rendered(b),
            vec!["--disable-sync", "--lang=ko-KR"],
            "user lang wins; an unset default key is still filled in"
        );
    }

    #[test]
    fn arg_defaults_fills_only_absent_keys() {
        let mut b = ArgsBuilder::new();
        b.arg_defaults([Arg::value("lang", "en_US")]);
        assert!(b.has("lang"));
        assert_eq!(rendered(b), vec!["--lang=en_US"]);
    }

    #[test]
    fn arg_defaults_unions_list_switches_with_user_values() {
        // A comma-list switch keeps BOTH the user flag and chromiumoxide's
        // built-in default (unlike a scalar switch, which the user replaces).
        let mut b = ArgsBuilder::new();
        b.arg(Arg::value("disable-features", "MyFeature"))
            .arg_defaults([Arg::value("disable-features", "TranslateUI")]);
        let rendered = rendered(b);
        assert_eq!(rendered.len(), 1);
        assert!(rendered[0].starts_with("--disable-features="));
        assert!(rendered[0].contains("MyFeature"), "user flag kept: {rendered:?}");
        assert!(
            rendered[0].contains("TranslateUI"),
            "default flag kept: {rendered:?}"
        );
    }
}
