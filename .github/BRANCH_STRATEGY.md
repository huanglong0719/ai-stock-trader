# Git 分支策略

## 分支类型

### 1. 主要分支
- **master**: 主分支，保持稳定，只接受经过测试的代码
- **develop**: 开发分支，集成最新功能

### 2. 功能分支
- **feature/**: 功能开发分支
  - 命名: `feature/功能描述`
  - 从: develop
  - 合并到: develop
  - 示例: `feature/user-auth`, `feature/trading-algorithm`

### 3. 修复分支
- **fix/**: Bug修复分支
  - 命名: `fix/问题描述`
  - 从: develop
  - 合并到: develop
  - 示例: `fix/tdx-connection-leak`, `fix/data-sync-error`

### 4. 热修复分支
- **hotfix/**: 紧急修复分支
  - 命名: `hotfix/问题描述`
  - 从: master
  - 合并到: master, develop
  - 示例: `hotfix/critical-security-fix`

### 5. 发布分支
- **release/**: 发布准备分支
  - 命名: `release/v版本号`
  - 从: develop
  - 合并到: master, develop
  - 示例: `release/v1.0.0`

## 工作流程

### 功能开发流程
1. 从 develop 创建功能分支
2. 在功能分支上开发
3. 提交 Pull Request 到 develop
4. 代码审查
5. 合并到 develop

### Bug修复流程
1. 从 develop 创建修复分支
2. 修复问题
3. 提交 Pull Request 到 develop
4. 代码审查
5. 合并到 develop

### 发布流程
1. 从 develop 创建发布分支
2. 进行发布前测试和调整
3. 提交 Pull Request 到 master
4. 合并到 master 并打标签
5. 同时合并到 develop

## 分支管理规范

### 分支命名
- 使用小写字母
- 用连字符分隔单词
- 避免使用特殊字符
- 保持简洁明了

### 提交信息
- 遵循提交信息规范
- 清晰描述修改内容
- 关联相关 Issue

### Pull Request 要求
- 提供清晰的描述
- 包含测试结果
- 关联相关 Issue
- 通过 CI/CD 检查

## 权限管理

### 角色权限
- **管理员**: 可以合并到 master，删除分支
- **开发者**: 可以创建分支，提交 PR
- **审查者**: 可以审查代码，批准合并

### 保护分支
- master 分支:
  - 需要 Pull Request
  - 需要至少1个审查批准
  - 需要通过 CI/CD 检查
  - 禁止强制推送

develop 分支:
  - 需要 Pull Request
  - 需要至少1个审查批准
  - 需要通过 CI/CD 检查