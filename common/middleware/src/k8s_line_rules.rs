use crate::{Middleware, Status};
use http::types::body::LineBufferMut;
use regex::bytes::RegexSet;
use thiserror::Error;

//lazy_static! {
//    static ref K8S_FILE_REG: Regex = Regex::new(
//        r#"^/([a-z0-9A-Z\-.]+)_([a-z0-9A-Z\-.]+)_([a-z0-9A-Z\-.]+)-([a-z0-9]{64}).log$"#
//    ).unwrap_or_else(|e| panic!("K8S_FILE_REG Regex::new() failed: {}", e));
//}

pub struct K8sLineRules {
    namespace: RegexSet,
    pod_name: RegexSet,
}

#[derive(Clone, Debug, Error)]
pub enum K8sLineRulesError {
    #[error(transparent)]
    RegexError(regex::Error),
}

impl K8sLineRules {
    pub fn new(namespace: String, pod_name: String) -> Result<K8sLineRules, K8sLineRulesError> {
        let namespace_regex = format!(
            r#"^([a-z0-9A-Z\-.]+)_{}_([a-z0-9A-Z\-.]+)-([a-z0-9]{{64}}).log$"#,
            regex::escape(&namespace),
        );
        let pod_name_regex = format!(
            r#"^{}_([a-z0-9A-Z\-.]+)_([a-z0-9A-Z\-.]+)-([a-z0-9]{{64}}).log$"#,
            regex::escape(&pod_name),
        );
        Ok(K8sLineRules {
            namespace: RegexSet::new(&[namespace_regex]).map_err(K8sLineRulesError::RegexError)?,
            pod_name: RegexSet::new(&[pod_name_regex]).map_err(K8sLineRulesError::RegexError)?,
        })
    }

    fn process_line<'a>(
        &self,
        line: &'a mut dyn LineBufferMut,
    ) -> Status<&'a mut dyn LineBufferMut> {
        let value = line.get_line_buffer().unwrap();

        // If it doesn't match any inclusion rule -> skip
        //if !self.inclusion.is_empty() && !self.inclusion.is_match(value) {
        //    return Status::Skip;
        //}

        // If any exclusion rule matches -> skip
        if self.namespace.is_match(value) {
            return Status::Skip;
        }

        if self.pod_name.is_match(value) {
            return Status::Skip;
        }

        Status::Ok(line)
    }
}

impl Middleware for K8sLineRules {
    fn run(&self) {}

    fn process<'a>(&self, line: &'a mut dyn LineBufferMut) -> Status<&'a mut dyn LineBufferMut> {
        if self.namespace.is_empty() && self.pod_name.is_empty() {
            // Avoid unnecessary allocations when no rules were defined
            return Status::Ok(line);
        }

        match line.get_line_buffer() {
            None => Status::Skip,
            Some(_) => self.process_line(line),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_k8s_line_rule_new() {
        let test_file_path = r#"pod-name_namespace_socat-listener-63d7c40bf1ece5ff559f49ef2da8f01163df85f611027a9d4bf5fef6e1a643bc.log"#;

        let namespace = String::from("namespace");
        let pod_name = String::from("pod-name");
        let k8s_rules = K8sLineRules::new(namespace, pod_name).unwrap();

        assert!(k8s_rules.namespace.is_match(test_file_path.as_bytes()));
        assert!(k8s_rules.pod_name.is_match(test_file_path.as_bytes()));
    }
}
