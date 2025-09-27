# OneEngine 测试套件

本目录包含 OneEngine 的完整测试套件，验证系统的各个组件和集成功能。

## 📋 测试文件

### 集成测试
- **`comprehensive_system_integration_test.rs`** - 全面系统集成测试
  - 验证整个OneEngine系统的完整打通
  - 测试Task调度 → Driver调度 → Pipeline执行 → Pipeline调度框架 → Operator算子 → 湖仓列式内容读取
  - 确保每个环节都是生产级别的完整实现

- **`unified_execution_test.rs`** - 统一执行测试
  - 验证从Task调度到湖仓列式内容读取的完整打通
  - 测试端到端的执行流程
  - 验证系统集成的一致性

## 🧪 运行测试

### 运行所有测试
```bash
cargo test
```

### 运行特定测试
```bash
# 运行集成测试
cargo test comprehensive_system_integration_test
cargo test unified_execution_test

# 运行特定模块测试
cargo test --lib
cargo test --bins
cargo test --examples
```

### 运行测试并显示输出
```bash
cargo test -- --nocapture
```

## 📊 测试覆盖

测试套件覆盖以下关键功能：

### 核心功能
- ✅ Task调度系统
- ✅ Driver调度系统
- ✅ Pipeline执行引擎
- ✅ Pipeline调度框架
- ✅ Operator算子（向量化）
- ✅ 湖仓列式内容读取

### 性能特性
- ✅ 向量化执行
- ✅ SIMD优化
- ✅ 内存管理
- ✅ 事件驱动架构
- ✅ NUMA感知调度

### 集成验证
- ✅ 端到端数据流
- ✅ 组件间通信
- ✅ 错误处理
- ✅ 资源管理

## 🔧 测试开发指南

### 添加新测试
1. 在相应的模块文件中添加测试函数
2. 使用 `#[cfg(test)]` 标记测试模块
3. 遵循命名约定：`test_功能描述`

### 集成测试
1. 在 `tests/` 目录中创建新的测试文件
2. 测试文件应该是一个独立的crate
3. 使用 `use oneengine::` 导入需要测试的模块

### 测试最佳实践
- 测试应该独立且可重复
- 使用有意义的测试名称
- 包含正面和负面测试用例
- 验证错误条件和边界情况

## 📝 测试报告

详细的测试报告请参考 [docs/INTEGRATION_TEST_REPORT.md](../docs/INTEGRATION_TEST_REPORT.md)。
