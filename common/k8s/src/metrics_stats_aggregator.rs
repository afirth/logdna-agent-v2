use anyhow::Result;
use core::result::Result::Ok;

use k8s_openapi::api::core::v1::{Container, ContainerStatus, Node, Pod};
use kube::{
    api::{Api, DynamicObject, GroupVersionKind, ListParams, ObjectList},
    discovery::{self},
    Client,
};
use serde_json::Value;

use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;

use crate::kube_stats::{
    cluster_stats::ClusterStats,
    container_stats::ContainerStats,
    controller_stats::ControllerStats,
    extended_pod_stats::ExtendedPodStats,
    node_stats::{NodeContainerStats, NodePodStats, NodeStats},
    pod_stats::PodStats,
};

pub struct MetricsServerAggregator {
    pub client: Client,
}

impl MetricsServerAggregator {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub async fn start_metrics_call_task(self) {
        loop {
            let result = self::process_reporter_info(self.client.clone()).await;

            match result {
                Ok(_) => {}
                Err(e) => {
                    error!("Failed To Gather Metrics Server Info {}", e)
                }
            }

            sleep(Duration::from_millis(30000)).await;
        }
    }
}

async fn process_reporter_info(client: Client) -> anyhow::Result<()> {
    let pods = self::get_all_pods(client.clone()).await?;
    let nodes = self::get_all_nodes(client.clone()).await?;
    let pod_metrics = self::call_metric_api(&"PodMetrics", client.clone()).await?;
    let node_metrics = self::call_metric_api(&"NodeMetrics", client.clone()).await?;

    let mut controller_map: HashMap<String, ControllerStats> = HashMap::new();
    let mut node_pod_counts_map: HashMap<String, NodePodStats> = HashMap::new();
    let mut node_container_counts_map: HashMap<String, NodeContainerStats> = HashMap::new();
    let mut pod_usage_map: HashMap<String, Value> = HashMap::new();
    let mut node_usage_map: HashMap<String, Value> = HashMap::new();

    let mut extended_pod_stats: Vec<ExtendedPodStats> = Vec::new();
    let mut node_stats: Vec<NodeStats> = Vec::new();

    build_pod_metric_map(pod_metrics, &mut pod_usage_map);
    process_pods(
        pods,
        &mut controller_map,
        pod_usage_map,
        &mut extended_pod_stats,
        &mut node_pod_counts_map,
        &mut node_container_counts_map,
    );
    print_pods(extended_pod_stats, controller_map);

    build_node_metric_map(node_metrics, &mut node_usage_map);
    process_nodes(
        nodes,
        node_usage_map,
        &mut node_stats,
        &mut node_pod_counts_map,
        &mut node_container_counts_map,
    );
    print_nodes(&node_stats);

    let cluster_stats = build_cluster_stats(&node_stats);
    print_cluster_stats(&cluster_stats);

    Ok(())
}

fn build_pod_metric_map(
    pod_metrics: ObjectList<DynamicObject>,
    pod_usage_map: &mut HashMap<String, Value>,
) {
    for pod_metric in pod_metrics {
        let containers = pod_metric.data["containers"].as_array();

        if containers.is_some() {
            for container in containers.unwrap() {
                let container_name = container["name"].as_str();

                if container_name.is_none() {
                    continue;
                }

                pod_usage_map.insert(
                    container_name.unwrap().to_string(),
                    container["usage"].clone(),
                );
            }
        }
    }
}

fn build_node_metric_map(
    node_metrics: ObjectList<DynamicObject>,
    node_usage_map: &mut HashMap<String, Value>,
) {
    for node_metric in node_metrics {
        let node_name = node_metric
            .metadata
            .name
            .unwrap_or_else(|| "NONE".to_string());
        let usage = &node_metric.data["usage"];

        node_usage_map.insert(node_name, usage.clone());
    }
}

fn build_cluster_stats(node_stats: &Vec<NodeStats>) -> ClusterStats {
    let mut cluster_stats = ClusterStats::new();

    for node_stat in node_stats {
        cluster_stats.containers_init += node_stat.containers_init;
        cluster_stats.containers_ready += node_stat.containers_ready;
        cluster_stats.containers_running += node_stat.containers_running;
        cluster_stats.containers_terminated += node_stat.containers_terminated;
        cluster_stats.containers_total += node_stat.containers_total;
        cluster_stats.containers_waiting += node_stat.containers_waiting;
        cluster_stats.cpu_allocatable += node_stat.cpu_allocatable.unwrap_or(0);
        cluster_stats.cpu_capacity += node_stat.cpu_capacity.unwrap_or(0);
        cluster_stats.cpu_usage += node_stat.cpu_usage;
        cluster_stats.memory_allocatable += node_stat.memory_allocatable.unwrap_or(0);
        cluster_stats.memory_capacity += node_stat.memory_capacity.unwrap_or(0);
        cluster_stats.memory_usage += node_stat.memory_usage;
        cluster_stats.pods_allocatable += node_stat.pods_allocatable.unwrap_or(0);
        cluster_stats.pods_capacity += node_stat.pods_capacity.unwrap_or(0);
        cluster_stats.pods_failed += node_stat.pods_failed;
        cluster_stats.pods_pending += node_stat.pods_pending;
        cluster_stats.pods_running += node_stat.pods_running;
        cluster_stats.pods_succeeded += node_stat.pods_succeeded;
        cluster_stats.pods_total += node_stat.pods_total;
        cluster_stats.pods_unknown += node_stat.pods_unknown;
        cluster_stats.nodes_total += 1;

        if node_stat.ready.unwrap_or(false) {
            cluster_stats.nodes_ready += 1;
        } else {
            cluster_stats.nodes_notready += 1;
        }

        if node_stat.unschedulable.unwrap_or(false) {
            cluster_stats.nodes_unschedulable += 1;
        }
    }

    cluster_stats
}

fn print_pods(
    extended_pod_stats: Vec<ExtendedPodStats>,
    controller_map: HashMap<String, ControllerStats>,
) {
    for mut translated_pod_container in extended_pod_stats {
        let controller_key = format!(
            "{}.{}.{}",
            translated_pod_container.pod_stats.namespace.clone(),
            translated_pod_container.pod_stats.controller_type.clone(),
            translated_pod_container.pod_stats.controller.clone()
        );

        let controller_stats = controller_map.get(&controller_key);

        if controller_stats.is_some() {
            translated_pod_container
                .controller_stats
                .copy_stats(controller_stats.unwrap());
        }

        println!(
            r#"{{"kube":{}}}"#,
            serde_json::to_string(&translated_pod_container).unwrap_or(String::from(""))
        );
    }
}

fn print_nodes(nodes: &Vec<NodeStats>) {
    for node in nodes {
        println!(
            r#"{{"kube":{}}}"#, 
            serde_json::to_string(&node).unwrap_or(String::from(""))
        );
    }
}

fn print_cluster_stats(cluster_stats: &ClusterStats) {
    println!(
        r#"{{"kube":{}}}"#, 
        serde_json::to_string(&cluster_stats).unwrap_or(String::from(""))
    )
}

fn process_pods(
    pods: ObjectList<Pod>,
    controller_map: &mut HashMap<String, ControllerStats>,
    pod_usage_map: HashMap<String, Value>,
    extended_pod_stats: &mut Vec<ExtendedPodStats>,
    node_pod_counts_map: &mut HashMap<String, NodePodStats>,
    node_container_counts_map: &mut HashMap<String, NodeContainerStats>,
) {
    for pod in pods {
        if pod.spec.is_none() || pod.status.is_none() {
            continue;
        }

        let status = pod.status.as_ref().unwrap();
        let spec = pod.spec.as_ref().unwrap();

        if status.conditions.is_none() || status.container_statuses.is_none() {
            continue;
        }

        let translated_pod = PodStats::builder(&pod).build();

        let node = translated_pod.node.clone();
        let phase = translated_pod.phase.clone();

        let node_pod_stat = node_pod_counts_map
            .entry(node.clone())
            .or_insert(NodePodStats::new());
        node_pod_stat.inc(&phase);

        let controller_key = format!(
            "{}.{}.{}",
            translated_pod.namespace.clone(),
            translated_pod.controller_type.clone(),
            translated_pod.controller.clone()
        );

        let controller = controller_map
            .entry(controller_key.clone())
            .or_insert(ControllerStats::new());

        let conditions = status.conditions.as_ref().unwrap();
        if conditions
            .iter()
            .any(|c| c.status.to_lowercase() == "true" && c.type_.to_lowercase() == "ready")
        {
            controller.inc_pods_ready();
        }

        controller.inc_pods_total();

        let mut container_status_map = HashMap::new();

        let default_status_vec = Vec::new();
        for status in status
            .container_statuses
            .as_ref()
            .unwrap_or_else(|| &&default_status_vec)
            .into_iter()
            .chain(
                status
                    .init_container_statuses
                    .as_ref()
                    .unwrap_or_else(|| &default_status_vec)
                    .into_iter(),
            )
        {
            container_status_map.insert(status.name.clone(), status.clone());

            let controller = controller_map
                .entry(controller_key.clone())
                .or_insert(ControllerStats::new());

            controller.inc_containers_total();

            if status.ready {
                controller.inc_containers_ready();
            }
        }

        for container in spec.containers.iter() {
            if container.name.is_empty()
                || container.image.is_none()
                || container.resources.is_none()
            {
                continue;
            }

            let container_status = container_status_map.get(&container.name);

            if container_status.is_none() {
                continue;
            }

            populate_container(
                &pod_usage_map,
                &container,
                container_status,
                node_container_counts_map,
                &node,
                extended_pod_stats,
                &translated_pod,
                false,
            );
        }

        let default_container_vec: Vec<Container> = Vec::new();
        for init_container in spec
            .init_containers
            .as_ref()
            .unwrap_or_else(|| &default_container_vec)
        {
            if init_container.name.is_empty()
                || init_container.image.is_none()
                || init_container.resources.is_none()
            {
                continue;
            }

            let container_status = container_status_map.get(&init_container.name);

            if container_status.is_none() {
                continue;
            }

            populate_container(
                &pod_usage_map,
                &init_container,
                container_status,
                node_container_counts_map,
                &node,
                extended_pod_stats,
                &translated_pod,
                true,
            );
        }
    }
}

fn populate_container(
    pod_usage_map: &HashMap<String, Value>,
    container: &Container,
    container_status: Option<&ContainerStatus>,
    node_container_counts_map: &mut HashMap<String, NodeContainerStats>,
    node: &String,
    extended_pod_stats: &mut Vec<ExtendedPodStats>,
    translated_pod: &PodStats,
    init: bool,
) {
    let usage = pod_usage_map.get(&container.name);
    if usage.is_some() {
        let translated_container = ContainerStats::builder(
            &container,
            container_status.as_ref().unwrap(),
            container_status.unwrap().state.as_ref().unwrap(),
            usage.unwrap()["cpu"].as_str().unwrap_or(""),
            usage.unwrap()["memory"].as_str().unwrap_or(""),
        )
        .build();

        let node_container_stat = node_container_counts_map
            .entry(node.clone())
            .or_insert(NodeContainerStats::new());

        node_container_stat.inc(
            &translated_container.state,
            translated_container.ready,
            init,
        );

        extended_pod_stats.push(ExtendedPodStats::new(
            translated_pod.clone(),
            translated_container,
        ));
    }
}

fn process_nodes(
    nodes: ObjectList<Node>,
    node_usage_map: HashMap<String, Value>,
    output_node_vec: &mut Vec<NodeStats>,
    node_pod_counts_map: &mut HashMap<String, NodePodStats>,
    node_container_counts_map: &mut HashMap<String, NodeContainerStats>,
) {
    for node in nodes {
        if node.spec.is_none() || node.status.is_none() || node.metadata.name.is_none() {
            continue;
        }

        let name = node.metadata.name.as_ref().unwrap();

        let default_node_container_stats = NodeContainerStats::new();
        let default_pod_container_stats = NodePodStats::new();

        let node_container_stats = node_container_counts_map
            .get(name)
            .unwrap_or_else(|| &default_node_container_stats);
        let node_pod_stats = node_pod_counts_map
            .get(name)
            .unwrap_or_else(|| &default_pod_container_stats);

        let usage = node_usage_map.get(name);
        if usage.is_some() {
            let translated_node = NodeStats::builder(
                &node,
                &node_pod_stats,
                &node_container_stats,
                usage.unwrap()["cpu"].as_str().unwrap_or(""),
                usage.unwrap()["memory"].as_str().unwrap_or(""),
            )
            .build();

            output_node_vec.push(translated_node);
        }
    }
}

async fn call_metric_api(
    kind: &str,
    client: Client,
) -> Result<ObjectList<DynamicObject>, kube::Error> {
    let gvk = GroupVersionKind::gvk("metrics.k8s.io", "v1beta1", kind);
    let (ar, _caps) = discovery::pinned_kind(&client, &gvk).await?;
    let api = Api::<DynamicObject>::all_with(client, &ar);

    let items = api.list(&ListParams::default()).await;

    items
}

async fn get_all_nodes(client: Client) -> Result<ObjectList<Node>, kube::Error> {
    let api: Api<Node> = Api::all(client);
    let nodes = api.list(&ListParams::default()).await;

    nodes
}

async fn get_all_pods(client: Client) -> Result<ObjectList<Pod>, kube::Error> {
    let api: Api<Pod> = Api::all(client);
    let pods = api.list(&ListParams::default()).await;

    pods
}
