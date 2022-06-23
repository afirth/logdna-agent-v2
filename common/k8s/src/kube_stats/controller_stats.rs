use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct ControllerStats {
    pub containers_ready: i32,
    pub containers_total: i32,
    pub pods_ready: i32,
    pub pods_total: i32,
}

impl ControllerStats {
    pub fn new() -> Self {
        Self {
            containers_ready: 0,
            containers_total: 0,
            pods_ready: 0,
            pods_total: 0,
        }
    }

    pub fn inc_containers_ready(&mut self) {
        self.containers_ready += 1;
    }

    pub fn inc_containers_total(&mut self) {
        self.containers_total += 1;
    }

    pub fn inc_pods_ready(&mut self) {
        self.pods_ready += 1;
    }

    pub fn inc_pods_total(&mut self) {
        self.pods_total += 1;
    }

    pub fn copy_stats(&mut self, value: &ControllerStats) {
        self.containers_ready = value.containers_ready;
        self.containers_total = value.containers_total;
        self.pods_ready = value.pods_ready;
        self.pods_total = value.pods_total;
    }
}