use std::fmt::Debug;
use std::path::Path;
use std::str::FromStr;

use core::fmt;
use globber::{Error as PatternError, Pattern};
use pcre2::{bytes::Regex, Error as RegexError};
#[cfg(target_os = "linux")]
use std::os::unix::ffi::OsStrExt;
use os_str_bytes::OsStrBytes;

/// A list of rules
pub type RuleList = Vec<Box<dyn Rule + Send>>;

/// A trait for implementing a rule, see GlobRule/RegexRule for an example
pub trait Rule: Debug {
    /// Takes a value and returns true or false based on if it matches
    fn matches(&self, value: &Path) -> bool;
}

/// Used for representing matches on Rules
#[derive(PartialEq)]
pub enum Status {
    /// Failed due to not being included
    NotIncluded,
    /// Was included but matched an exclusion rule therefor it did not pass
    Excluded,
    /// Passed
    Ok,
}

impl Status {
    /// Converts a status into a bool, returning true if the status is ok and false otherwise
    pub fn is_ok(&self) -> bool {
        matches!(self, Status::Ok)
    }
}

/// Holds both exclusion and inclusion rules
#[derive(Default, Debug)]
pub struct Rules {
    inclusion: RuleList,
    exclusion: RuleList,
}

impl Rules {
    /// Constructs an empty instance of Rules
    pub fn new() -> Self {
        Self {
            inclusion: Vec::new(),
            exclusion: Vec::new(),
        }
    }
    /// Check if value is included (matches at least one inclusion rule)
    pub fn included(&self, value: &Path) -> Status {
        for rule in &self.inclusion {
            if rule.matches(value) {
                return Status::Ok;
            }
        }
        Status::NotIncluded
    }
    /// Check if value is excluded (matches none of the exclusion rules)
    pub fn excluded(&self, value: &Path) -> Status {
        for rule in &self.exclusion {
            if rule.matches(value) {
                return Status::Excluded;
            }
        }
        Status::Ok
    }
    /// Returns true if the value is included but not excluded
    pub fn passes(&self, value: &Path) -> Status {
        if self.included(value) == Status::NotIncluded {
            return Status::NotIncluded;
        }

        self.excluded(value)
    }
    /// Adds an inclusion rule
    pub fn add_inclusion<T: Rule + Send + 'static>(&mut self, rule: T) {
        self.inclusion.push(Box::new(rule))
    }
    /// Adds an exclusion rule
    pub fn add_exclusion<T: Rule + Send + 'static>(&mut self, rule: T) {
        self.exclusion.push(Box::new(rule))
    }
    /// Appends all rules from another instance of rules
    pub fn add_all<T: Into<Rules>>(&mut self, rules: T) {
        let mut rules = rules.into();
        self.exclusion.append(&mut rules.exclusion);
        self.inclusion.append(&mut rules.inclusion);
    }
    /// Getter for inclusion list
    pub fn inclusion_list(&self) -> &RuleList {
        &self.inclusion
    }
    /// Getter for exclusion list
    pub fn exclusion_list(&self) -> &RuleList {
        &self.exclusion
    }
}

/// A rule the matches it's input based on a Regex
#[derive(Debug)]
pub struct RegexRule {
    inner: Regex,
}

impl RegexRule {
    /// Creates a new RegexRule from a pattern
    pub fn new<'a, T: Into<&'a str>>(pattern: T) -> Result<Self, RegexError> {
        Ok(Self {
            inner: Regex::new(pattern.into())?,
        })
    }
}

impl Rule for RegexRule {
    fn matches(&self, value: &Path) -> bool {
        self.inner
            .is_match(value.as_os_str().to_raw_bytes().as_ref())
            .unwrap_or(false)
    }
}

impl FromStr for RegexRule {
    type Err = RegexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        RegexRule::new(s)
    }
}

/// A rule the matches it's input based on a Glob pattern, note extended glob is not supported
pub struct GlobRule {
    inner: Pattern,
    str_value: String,
}

impl fmt::Debug for GlobRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GlobRule")
            .field("inner", &self.str_value)
            .finish()
    }
}

impl GlobRule {
    /// Creates a new GlobRule from a pattern
    pub fn new<'a, T: Into<&'a str>>(pattern: T) -> Result<Self, PatternError> {
        let str_value = pattern.into().to_string();
        Ok(Self {
            inner: Pattern::new(&str_value)?,
            str_value,
        })
    }
}

impl Rule for GlobRule {
    fn matches(&self, value: &Path) -> bool {
        self.inner.matches(&value.to_string_lossy())
    }
}

impl FromStr for GlobRule {
    type Err = PatternError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        GlobRule::new(s)
    }
}
