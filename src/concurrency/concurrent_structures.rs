/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


//! 并发优化
//! 
//! crossbeam/loom竞态测试，无锁ring buffer，MPSC批量唤醒

use std::sync::atomic::{AtomicUsize, AtomicPtr, Ordering};
use std::sync::{Arc, Mutex, Condvar};
use std::thread;
use std::time::{Duration, Instant};
use std::collections::VecDeque;
use std::ptr::null_mut;

/// 无锁环形缓冲区
pub struct LockFreeRingBuffer<T> {
    /// 缓冲区
    buffer: Vec<T>,
    /// 写入位置
    write_pos: AtomicUsize,
    /// 读取位置
    read_pos: AtomicUsize,
    /// 容量
    capacity: usize,
    /// 掩码（用于快速取模）
    mask: usize,
}

/// MPSC（多生产者单消费者）队列
pub struct MpscQueue<T> {
    /// 头指针
    head: AtomicPtr<Node<T>>,
    /// 尾指针
    tail: AtomicPtr<Node<T>>,
    /// 节点池
    node_pool: Arc<NodePool<T>>,
}

/// 队列节点
pub struct Node<T> {
    /// 数据
    data: Option<T>,
    /// 下一个节点
    next: AtomicPtr<Node<T>>,
}

/// 节点池
pub struct NodePool<T> {
    /// 空闲节点
    free_nodes: Mutex<VecDeque<Box<Node<T>>>>,
    /// 条件变量
    condvar: Condvar,
    /// 最大池大小
    max_pool_size: usize,
}

/// 批量唤醒器
pub struct BatchWaker {
    /// 等待的线程
    waiting_threads: Mutex<VecDeque<thread::Thread>>,
    /// 条件变量
    condvar: Condvar,
    /// 批量大小
    batch_size: usize,
    /// 唤醒间隔
    wake_interval: Duration,
}

/// 竞态测试器
pub struct RaceTester {
    /// 测试配置
    config: RaceTestConfig,
    /// 测试结果
    results: Arc<Mutex<Vec<RaceTestResult>>>,
}

/// 竞态测试配置
#[derive(Debug, Clone)]
pub struct RaceTestConfig {
    /// 线程数量
    thread_count: usize,
    /// 操作数量
    operation_count: usize,
    /// 测试持续时间
    duration: Duration,
    /// 是否启用loom测试
    enable_loom: bool,
}

/// 竞态测试结果
#[derive(Debug, Clone)]
pub struct RaceTestResult {
    /// 测试名称
    test_name: String,
    /// 执行时间
    execution_time: Duration,
    /// 操作次数
    operation_count: usize,
    /// 错误次数
    error_count: usize,
    /// 吞吐量
    throughput: f64,
    /// 是否通过
    passed: bool,
}

/// 无锁哈希表
pub struct LockFreeHashMap<K, V> {
    /// 桶数组
    buckets: Vec<AtomicPtr<Bucket<K, V>>>,
    /// 桶数量
    bucket_count: usize,
    /// 掩码
    mask: usize,
}

/// 哈希表桶
pub struct Bucket<K, V> {
    /// 键
    key: K,
    /// 值
    value: V,
    /// 下一个桶
    next: AtomicPtr<Bucket<K, V>>,
}

/// 工作窃取队列
pub struct WorkStealingQueue<T> {
    /// 队列数组
    queues: Vec<VecDeque<T>>,
    /// 队列锁
    queue_locks: Vec<Mutex<()>>,
    /// 队列数量
    queue_count: usize,
}

impl<T> LockFreeRingBuffer<T> {
    /// 创建新的无锁环形缓冲区
    pub fn new(capacity: usize) -> Self {
        // 确保容量是2的幂
        let actual_capacity = capacity.next_power_of_two();
        let mut buffer = Vec::with_capacity(actual_capacity);
        buffer.resize_with(actual_capacity, || unsafe { std::mem::zeroed() });

        Self {
            buffer,
            write_pos: AtomicUsize::new(0),
            read_pos: AtomicUsize::new(0),
            capacity: actual_capacity,
            mask: actual_capacity - 1,
        }
    }

    /// 尝试推送元素
    pub fn try_push(&self, item: T) -> Result<(), T> {
        let write_pos = self.write_pos.load(Ordering::Relaxed);
        let read_pos = self.read_pos.load(Ordering::Relaxed);
        
        // 检查是否已满
        if (write_pos + 1) & self.mask == read_pos & self.mask {
            return Err(item);
        }

        // 写入元素
        unsafe {
            let ptr = self.buffer.as_ptr().add(write_pos & self.mask);
            std::ptr::write(ptr, item);
        }

        // 更新写入位置
        self.write_pos.store(write_pos + 1, Ordering::Release);
        Ok(())
    }

    /// 尝试弹出元素
    pub fn try_pop(&self) -> Option<T> {
        let read_pos = self.read_pos.load(Ordering::Relaxed);
        let write_pos = self.write_pos.load(Ordering::Acquire);
        
        // 检查是否为空
        if read_pos == write_pos {
            return None;
        }

        // 读取元素
        let item = unsafe {
            let ptr = self.buffer.as_ptr().add(read_pos & self.mask);
            std::ptr::read(ptr)
        };

        // 更新读取位置
        self.read_pos.store(read_pos + 1, Ordering::Release);
        Some(item)
    }

    /// 获取当前大小
    pub fn len(&self) -> usize {
        let write_pos = self.write_pos.load(Ordering::Relaxed);
        let read_pos = self.read_pos.load(Ordering::Relaxed);
        (write_pos - read_pos) & self.mask
    }

    /// 检查是否为空
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 检查是否已满
    pub fn is_full(&self) -> bool {
        self.len() == self.capacity - 1
    }
}

impl<T> MpscQueue<T> {
    /// 创建新的MPSC队列
    pub fn new(max_pool_size: usize) -> Self {
        let node_pool = Arc::new(NodePool::new(max_pool_size));
        let dummy = Box::new(Node {
            data: None,
            next: AtomicPtr::new(null_mut()),
        });
        let dummy_ptr = Box::into_raw(dummy);
        
        Self {
            head: AtomicPtr::new(dummy_ptr),
            tail: AtomicPtr::new(dummy_ptr),
            node_pool,
        }
    }

    /// 推送元素
    pub fn push(&self, item: T) -> Result<(), T> {
        // 从节点池获取节点
        let node = match self.node_pool.get_node() {
            Some(mut node) => {
                node.data = Some(item);
                Box::into_raw(node)
            },
            None => {
                // 节点池为空，创建新节点
                let node = Box::new(Node {
                    data: Some(item),
                    next: AtomicPtr::new(null_mut()),
                });
                Box::into_raw(node)
            }
        };

        // 原子地更新尾指针
        let prev_tail = self.tail.swap(node, Ordering::AcqRel);
        unsafe {
            (*prev_tail).next.store(node, Ordering::Release);
        }

        Ok(())
    }

    /// 弹出元素
    pub fn pop(&self) -> Option<T> {
        let head = self.head.load(Ordering::Acquire);
        let next = unsafe { (*head).next.load(Ordering::Acquire) };
        
        if next.is_null() {
            return None;
        }

        let data = unsafe {
            let next_node = &*next;
            next_node.data.take()
        };

        // 更新头指针
        self.head.store(next, Ordering::Release);
        
        // 将旧头节点返回池中
        self.node_pool.return_node(unsafe { Box::from_raw(head) });

        data
    }

    /// 检查是否为空
    pub fn is_empty(&self) -> bool {
        let head = self.head.load(Ordering::Acquire);
        let next = unsafe { (*head).next.load(Ordering::Acquire) };
        next.is_null()
    }
}

impl<T> NodePool<T> {
    /// 创建新的节点池
    pub fn new(max_pool_size: usize) -> Self {
        Self {
            free_nodes: Mutex::new(VecDeque::new()),
            condvar: Condvar::new(),
            max_pool_size,
        }
    }

    /// 获取节点
    pub fn get_node(&self) -> Option<Box<Node<T>>> {
        let mut free_nodes = self.free_nodes.lock().unwrap();
        free_nodes.pop_front()
    }

    /// 返回节点
    pub fn return_node(&self, mut node: Box<Node<T>>) {
        let mut free_nodes = self.free_nodes.lock().unwrap();
        
        if free_nodes.len() < self.max_pool_size {
            node.data = None;
            node.next = AtomicPtr::new(null_mut());
            free_nodes.push_back(node);
            self.condvar.notify_one();
        }
    }
}

impl BatchWaker {
    /// 创建新的批量唤醒器
    pub fn new(batch_size: usize, wake_interval: Duration) -> Self {
        Self {
            waiting_threads: Mutex::new(VecDeque::new()),
            condvar: Condvar::new(),
            batch_size,
            wake_interval,
        }
    }

    /// 等待唤醒
    pub fn wait(&self) {
        let mut waiting_threads = self.waiting_threads.lock().unwrap();
        waiting_threads.push_back(thread::current());
        
        while waiting_threads.contains(&thread::current()) {
            waiting_threads = self.condvar.wait(waiting_threads).unwrap();
        }
    }

    /// 批量唤醒
    pub fn wake_batch(&self) {
        let mut waiting_threads = self.waiting_threads.lock().unwrap();
        let mut wake_count = 0;
        
        while wake_count < self.batch_size && !waiting_threads.is_empty() {
            if let Some(thread) = waiting_threads.pop_front() {
                thread.unpark();
                wake_count += 1;
            }
        }
        
        if !waiting_threads.is_empty() {
            self.condvar.notify_all();
        }
    }

    /// 启动批量唤醒循环
    pub fn start_batch_waker(&self) {
        let waker = self.clone();
        thread::spawn(move || {
            loop {
                thread::sleep(waker.wake_interval);
                waker.wake_batch();
            }
        });
    }
}

impl Clone for BatchWaker {
    fn clone(&self) -> Self {
        Self {
            waiting_threads: Mutex::new(VecDeque::new()),
            condvar: Condvar::new(),
            batch_size: self.batch_size,
            wake_interval: self.wake_interval,
        }
    }
}

impl RaceTester {
    /// 创建新的竞态测试器
    pub fn new(config: RaceTestConfig) -> Self {
        Self {
            config,
            results: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// 运行竞态测试
    pub fn run_race_test<F>(&self, test_name: &str, test_func: F) -> RaceTestResult
    where
        F: Fn() -> Result<(), String> + Send + Sync + 'static,
    {
        let start_time = Instant::now();
        let mut operation_count = 0;
        let mut error_count = 0;
        let mut handles = Vec::new();

        // 创建测试线程
        for _ in 0..self.config.thread_count {
            let test_func = Arc::new(test_func);
            let results = self.results.clone();
            
            let handle = thread::spawn(move || {
                let mut local_operations = 0;
                let mut local_errors = 0;
                
                for _ in 0..self.config.operation_count {
                    match test_func() {
                        Ok(_) => local_operations += 1,
                        Err(_) => local_errors += 1,
                    }
                }
                
                (local_operations, local_errors)
            });
            
            handles.push(handle);
        }

        // 等待所有线程完成
        for handle in handles {
            let (ops, errors) = handle.join().unwrap();
            operation_count += ops;
            error_count += errors;
        }

        let execution_time = start_time.elapsed();
        let throughput = operation_count as f64 / execution_time.as_secs_f64();
        let passed = error_count == 0;

        let result = RaceTestResult {
            test_name: test_name.to_string(),
            execution_time,
            operation_count,
            error_count,
            throughput,
            passed,
        };

        // 保存结果
        let mut results = self.results.lock().unwrap();
        results.push(result.clone());

        result
    }

    /// 获取所有测试结果
    pub fn get_results(&self) -> Vec<RaceTestResult> {
        self.results.lock().unwrap().clone()
    }
}

impl<K, V> LockFreeHashMap<K, V>
where
    K: Eq + std::hash::Hash + Clone,
    V: Clone,
{
    /// 创建新的无锁哈希表
    pub fn new(bucket_count: usize) -> Self {
        let actual_bucket_count = bucket_count.next_power_of_two();
        let mut buckets = Vec::with_capacity(actual_bucket_count);
        
        for _ in 0..actual_bucket_count {
            buckets.push(AtomicPtr::new(null_mut()));
        }

        Self {
            buckets,
            bucket_count: actual_bucket_count,
            mask: actual_bucket_count - 1,
        }
    }

    /// 插入键值对
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let hash = self.hash(&key);
        let bucket_index = hash & self.mask;
        
        let new_bucket = Box::new(Bucket {
            key: key.clone(),
            value: value.clone(),
            next: AtomicPtr::new(null_mut()),
        });
        let new_bucket_ptr = Box::into_raw(new_bucket);

        // 原子地更新桶
        let prev_bucket = self.buckets[bucket_index].swap(new_bucket_ptr, Ordering::AcqRel);
        
        if !prev_bucket.is_null() {
            unsafe {
                (*new_bucket_ptr).next.store(prev_bucket, Ordering::Release);
            }
        }

        // 检查是否有相同的键被替换
        let mut current = prev_bucket;
        while !current.is_null() {
            unsafe {
                let bucket = &*current;
                if bucket.key == key {
                    // 找到相同键，返回旧值
                    return Some(bucket.value.clone());
                }
                current = bucket.next.load(Ordering::Acquire);
            }
        }

        // 没有找到相同键，返回None
        None
    }

    /// 获取值
    pub fn get(&self, key: &K) -> Option<V> {
        let hash = self.hash(key);
        let bucket_index = hash & self.mask;
        
        let mut current = self.buckets[bucket_index].load(Ordering::Acquire);
        
        while !current.is_null() {
            unsafe {
                let bucket = &*current;
                if bucket.key == *key {
                    return Some(bucket.value.clone());
                }
                current = bucket.next.load(Ordering::Acquire);
            }
        }

        None
    }

    /// 计算哈希值
    fn hash(&self, key: &K) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize
    }
}

impl<T> WorkStealingQueue<T> {
    /// 创建新的工作窃取队列
    pub fn new(queue_count: usize) -> Self {
        let mut queues = Vec::with_capacity(queue_count);
        let mut queue_locks = Vec::with_capacity(queue_count);
        
        for _ in 0..queue_count {
            queues.push(VecDeque::new());
            queue_locks.push(Mutex::new(()));
        }

        Self {
            queues,
            queue_locks,
            queue_count,
        }
    }

    /// 推送任务到指定队列
    pub fn push(&self, queue_id: usize, task: T) -> Result<(), T> {
        if queue_id >= self.queue_count {
            return Err(task);
        }

        let _lock = self.queue_locks[queue_id].lock().unwrap();
        self.queues[queue_id].push_back(task);
        Ok(())
    }

    /// 从指定队列弹出任务
    pub fn pop(&self, queue_id: usize) -> Option<T> {
        if queue_id >= self.queue_count {
            return None;
        }

        let _lock = self.queue_locks[queue_id].lock().unwrap();
        self.queues[queue_id].pop_front()
    }

    /// 窃取任务
    pub fn steal(&self, from_queue_id: usize, to_queue_id: usize) -> Option<T> {
        if from_queue_id >= self.queue_count || to_queue_id >= self.queue_count {
            return None;
        }

        if from_queue_id == to_queue_id {
            return self.pop(from_queue_id);
        }

        // 尝试从其他队列窃取
        let _lock_from = self.queue_locks[from_queue_id].lock().unwrap();
        if let Some(task) = self.queues[from_queue_id].pop_back() {
            return Some(task);
        }

        None
    }
}

impl Default for RaceTestConfig {
    fn default() -> Self {
        Self {
            thread_count: 4,
            operation_count: 1000,
            duration: Duration::from_secs(10),
            enable_loom: false,
        }
    }
}

impl Default for BatchWaker {
    fn default() -> Self {
        Self::new(10, Duration::from_millis(1))
    }
}
