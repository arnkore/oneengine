use crate::core::task::ResourceRequirements;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};
use uuid::Uuid;

/// Manages system resources and allocations
pub struct ResourceManager {
    total_resources: ResourcePool,
    allocated_resources: Arc<RwLock<HashMap<Uuid, ResourceAllocation>>>,
    resource_config: ResourceConfig,
}

/// Available resource pool
#[derive(Debug, Clone)]
pub struct ResourcePool {
    pub cpu_cores: u32,
    pub memory_mb: u64,
    pub disk_mb: u64,
    pub network_bandwidth_mbps: u32,
    pub gpu_cores: u32,
    pub custom_resources: HashMap<String, u64>,
}

/// Resource allocation for a specific task
#[derive(Debug, Clone)]
pub struct ResourceAllocation {
    pub allocation_id: Uuid,
    pub cpu_cores: u32,
    pub memory_mb: u64,
    pub disk_mb: u64,
    pub network_bandwidth_mbps: Option<u32>,
    pub gpu_cores: Option<u32>,
    pub custom_resources: HashMap<String, u64>,
}

/// Resource management configuration
#[derive(Debug, Clone)]
pub struct ResourceConfig {
    pub max_cpu_utilization: f64,
    pub max_memory_utilization: f64,
    pub enable_gpu_scheduling: bool,
    pub enable_custom_resources: bool,
}

impl Default for ResourceConfig {
    fn default() -> Self {
        Self {
            max_cpu_utilization: 0.8,
            max_memory_utilization: 0.8,
            enable_gpu_scheduling: false,
            enable_custom_resources: false,
        }
    }
}

impl ResourceManager {
    /// Create a new resource manager
    pub fn new(resource_config: ResourceConfig) -> Self {
        // Detect available system resources
        let total_resources = Self::detect_system_resources(&resource_config);
        
        Self {
            total_resources,
            allocated_resources: Arc::new(RwLock::new(HashMap::new())),
            resource_config,
        }
    }

    /// Detect available system resources
    fn detect_system_resources(config: &ResourceConfig) -> ResourcePool {
        // In a real implementation, this would detect actual system resources
        // For now, we'll use reasonable defaults
        ResourcePool {
            cpu_cores: num_cpus::get() as u32,
            memory_mb: 8192, // 8GB default
            disk_mb: 1024 * 1024, // 1TB default
            network_bandwidth_mbps: 1000, // 1Gbps default
            gpu_cores: if config.enable_gpu_scheduling { 2 } else { 0 },
            custom_resources: HashMap::new(),
        }
    }

    /// Check if resources can be allocated for the given requirements
    pub async fn can_allocate(&self, requirements: &ResourceRequirements) -> Result<bool> {
        let allocated = self.allocated_resources.read().await;
        
        // Calculate currently allocated resources
        let (allocated_cpu, allocated_memory, allocated_disk, allocated_gpu) = 
            allocated.values().fold((0, 0, 0, 0), |(cpu, mem, disk, gpu), alloc| {
                (
                    cpu + alloc.cpu_cores,
                    mem + alloc.memory_mb,
                    disk + alloc.disk_mb,
                    gpu + alloc.gpu_cores.unwrap_or(0),
                )
            });

        // Check if we have enough resources
        let available_cpu = (self.total_resources.cpu_cores as f64 * self.resource_config.max_cpu_utilization) as u32;
        let available_memory = (self.total_resources.memory_mb as f64 * self.resource_config.max_memory_utilization) as u64;
        let available_disk = self.total_resources.disk_mb;
        let available_gpu = self.total_resources.gpu_cores;

        let can_allocate = requirements.cpu_cores <= available_cpu.saturating_sub(allocated_cpu)
            && requirements.memory_mb <= available_memory.saturating_sub(allocated_memory)
            && requirements.disk_mb <= available_disk.saturating_sub(allocated_disk)
            && requirements.gpu_cores.unwrap_or(0) <= available_gpu.saturating_sub(allocated_gpu);

        debug!(
            "Resource check: required={:?}, available_cpu={}, allocated_cpu={}, can_allocate={}",
            requirements, available_cpu, allocated_cpu, can_allocate
        );

        Ok(can_allocate)
    }

    /// Allocate resources for a task
    pub async fn allocate(&self, requirements: &ResourceRequirements) -> Result<ResourceAllocation> {
        if !self.can_allocate(requirements).await? {
            return Err(anyhow::anyhow!("Insufficient resources for allocation"));
        }

        let allocation = ResourceAllocation {
            allocation_id: Uuid::new_v4(),
            cpu_cores: requirements.cpu_cores,
            memory_mb: requirements.memory_mb,
            disk_mb: requirements.disk_mb,
            network_bandwidth_mbps: requirements.network_bandwidth_mbps,
            gpu_cores: requirements.gpu_cores,
            custom_resources: requirements.custom_resources.clone(),
        };

        // Record the allocation
        {
            let mut allocated = self.allocated_resources.write().await;
            allocated.insert(allocation.allocation_id, allocation.clone());
        }

        debug!("Resources allocated: {:?}", allocation);
        Ok(allocation)
    }

    /// Deallocate resources
    pub async fn deallocate(&self, allocation_id: Uuid) -> Result<()> {
        let mut allocated = self.allocated_resources.write().await;
        
        if allocated.remove(&allocation_id).is_some() {
            debug!("Resources deallocated: {}", allocation_id);
            Ok(())
        } else {
            warn!("Attempted to deallocate non-existent allocation: {}", allocation_id);
            Err(anyhow::anyhow!("Allocation not found"))
        }
    }

    /// Get current resource utilization
    pub async fn get_utilization(&self) -> ResourceUtilization {
        let allocated = self.allocated_resources.read().await;
        
        let (allocated_cpu, allocated_memory, allocated_disk, allocated_gpu) = 
            allocated.values().fold((0, 0, 0, 0), |(cpu, mem, disk, gpu), alloc| {
                (
                    cpu + alloc.cpu_cores,
                    mem + alloc.memory_mb,
                    disk + alloc.disk_mb,
                    gpu + alloc.gpu_cores.unwrap_or(0),
                )
            });

        ResourceUtilization {
            cpu_utilization: allocated_cpu as f64 / self.total_resources.cpu_cores as f64,
            memory_utilization: allocated_memory as f64 / self.total_resources.memory_mb as f64,
            disk_utilization: allocated_disk as f64 / self.total_resources.disk_mb as f64,
            gpu_utilization: if self.total_resources.gpu_cores > 0 {
                allocated_gpu as f64 / self.total_resources.gpu_cores as f64
            } else {
                0.0
            },
            total_allocations: allocated.len(),
        }
    }

    /// Get available resources
    pub async fn get_available_resources(&self) -> ResourcePool {
        let allocated = self.allocated_resources.read().await;
        
        let (allocated_cpu, allocated_memory, allocated_disk, allocated_gpu) = 
            allocated.values().fold((0, 0, 0, 0), |(cpu, mem, disk, gpu), alloc| {
                (
                    cpu + alloc.cpu_cores,
                    mem + alloc.memory_mb,
                    disk + alloc.disk_mb,
                    gpu + alloc.gpu_cores.unwrap_or(0),
                )
            });

        ResourcePool {
            cpu_cores: self.total_resources.cpu_cores.saturating_sub(allocated_cpu),
            memory_mb: self.total_resources.memory_mb.saturating_sub(allocated_memory),
            disk_mb: self.total_resources.disk_mb.saturating_sub(allocated_disk),
            network_bandwidth_mbps: self.total_resources.network_bandwidth_mbps,
            gpu_cores: self.total_resources.gpu_cores.saturating_sub(allocated_gpu),
            custom_resources: self.total_resources.custom_resources.clone(),
        }
    }
}

/// Resource utilization statistics
#[derive(Debug, Clone)]
pub struct ResourceUtilization {
    pub cpu_utilization: f64,
    pub memory_utilization: f64,
    pub disk_utilization: f64,
    pub gpu_utilization: f64,
    pub total_allocations: usize,
}
