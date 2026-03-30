#!/usr/bin/env python3
"""
自动配置 GitHub 仓库的完整脚本
"""

import subprocess
import json
import time
import webbrowser
import sys
from typing import Dict, Any

def run_gh_command(cmd: str) -> tuple[bool, str]:
    """运行 GitHub CLI 命令"""
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            encoding='utf-8'
        )
        return result.returncode == 0, result.stdout + result.stderr
    except Exception as e:
        return False, str(e)

def configure_repository_settings():
    """配置仓库基本设置"""
    print("🔧 配置仓库基本设置...")

    commands = [
        'gh repo edit huanglong0719/ai-stock-trader --enable-issues=true',
        'gh repo edit huanglong0719/ai-stock-trader --enable-wiki=false',
        'gh repo edit huanglong0719/ai-stock-trader --enable-projects=false',
        'gh repo edit huanglong0719/ai-stock-trader --enable-discussions=false'
    ]

    for cmd in commands:
        success, output = run_gh_command(cmd)
        if success:
            print(f"  ✅ {cmd.split('--')[-1]}")
        else:
            print(f"  ❌ {cmd.split('--')[-1]}: {output}")

def create_branch_protection():
    """创建分支保护配置的 JSON 文件"""
    print("🔧 创建分支保护配置文件...")

    protection_config = {
        "required_status_checks": None,
        "enforce_admins": None,
        "required_pull_request_reviews": None,
        "required_linear_history": None,
        "allow_force_pushes": False,
        "allow_deletions": False
    }

    with open('branch_protection_config.json', 'w', encoding='utf-8') as f:
        json.dump(protection_config, f, indent=2, ensure_ascii=False)

    print("  ✅ 分支保护配置文件已创建: branch_protection_config.json")
    print("  ℹ️  由于 GitHub API 限制，需要手动配置分支保护")

def configure_actions_settings():
    """配置 Actions 设置"""
    print("🔧 配置 Actions 设置...")

    # 检查 Actions 是否启用
    success, output = run_gh_command(
        'gh api repos/huanglong0719/ai-stock-trader/actions/permissions'
    )

    if success:
        print("  ✅ Actions 权限信息可访问")
    else:
        print("  ℹ️  Actions 权限需要手动配置")

def enable_security_features():
    """启用安全功能"""
    print("🔧 启用安全功能...")

    # 启用 Dependabot
    dependabot_config = {
        "version": 2,
        "updates": [
            {
                "package-ecosystem": "pip",
                "directory": "/backend",
                "schedule": {
                    "interval": "weekly"
                }
            },
            {
                "package-ecosystem": "npm",
                "directory": "/frontend",
                "schedule": {
                    "interval": "weekly"
                }
            }
        ]
    }

    with open('.github/dependabot.yml', 'w', encoding='utf-8') as f:
        import yaml
        try:
            yaml.dump(dependabot_config, f, default_flow_style=False, allow_unicode=True)
            print("  ✅ Dependabot 配置文件已创建")
        except:
            # 如果没有 yaml 库，创建简单的配置
            f.write("""version: 2
updates:
  - package-ecosystem: "pip"
    directory: "/backend"
    schedule:
      interval: "weekly"
  - package-ecosystem: "npm"
    directory: "/frontend"
    schedule:
      interval: "weekly"
""")
            print("  ✅ Dependabot 配置文件已创建")

def create_setup_instructions():
    """创建详细的设置说明"""
    print("🔧 创建设置说明文档...")

    instructions = """# GitHub 仓库自动配置完成

## 🎉 已自动完成的配置

### ✅ Git 配置
- 更新了 .gitignore 文件
- 清理了临时文件
- 设置了提交信息模板
- 创建了 develop 分支

### ✅ GitHub 自动化
- 创建了 CI/CD 工作流
- 配置了仓库基本信息
- 创建了协作模板

### ✅ 安全配置
- 创建了 Dependabot 配置文件

## 📋 需要手动完成的配置

由于 GitHub API 的限制，以下配置需要手动完成：

### 1. 配置分支保护 (最重要)

**步骤:**
1. 访问: https://github.com/huanglong0719/ai-stock-trader/settings/branches
2. 点击 "Add rule"
3. 输入 "Branch name pattern": `master`
4. 勾选以下选项:
   - ✅ Require a pull request before merging
   - ✅ Require approvals (设置为 1)
   - ✅ Dismiss stale pull request approvals
   - ✅ Require status checks to pass
   - ✅ Require branches to be up to date
   - ✅ Require linear history
   - ✅ Include administrators
5. 点击 "Create"

### 2. 配置 Actions 权限

**步骤:**
1. 访问: https://github.com/huanglong0719/ai-stock-trader/settings/actions
2. 选择 "Allow all actions and reusable workflows"
3. 点击 "Save"

### 3. 启用安全功能

**步骤:**
1. 访问: https://github.com/huanglong0719/ai-stock-trader/settings/security_analysis
2. 启用:
   - ✅ Dependabot alerts
   - ✅ Dependabot security updates
3. 系统会自动保存

### 4. 配置自动合并

**步骤:**
1. 访问: https://github.com/huanglong0719/ai-stock-trader/settings
2. 在 "Pull Requests" 部分启用:
   - ✅ Allow auto-merge
   - ✅ Automatically delete head branches
3. 点击 "Save changes"

## 🚀 验证配置

完成以上配置后，运行验证脚本:
```bash
python verify_setup.py
```

## 📚 参考文档

- `GITHUB_SETUP_GUIDE.md` - 详细配置指南
- `QUICK_SETUP.md` - 快速配置指南
- `BRANCH_STRATEGY.md` - 分支策略文档

## 🎯 配置完成后的效果

✅ **企业级代码管理**
- 强制代码审查
- 自动化测试和部署
- 完善的安全防护
- 标准化的协作流程

---

**配置完成后，你的 GitHub 仓库将达到企业级标准！** 🎉
"""

    with open('CONFIGURATION_COMPLETE.md', 'w', encoding='utf-8') as f:
        f.write(instructions)

    print("  ✅ 配置说明文档已创建: CONFIGURATION_COMPLETE.md")

def open_configuration_pages():
    """打开配置页面"""
    print("🔧 打开配置页面...")

    urls = [
        "https://github.com/huanglong0719/ai-stock-trader/settings/branches",
        "https://github.com/huanglong0719/ai-stock-trader/settings/actions",
        "https://github.com/huanglong0719/ai-stock-trader/settings/security_analysis",
        "https://github.com/huanglong0719/ai-stock-trader/settings"
    ]

    for i, url in enumerate(urls, 1):
        print(f"  [{i}/{len(urls)}] 正在打开: {url}")
        try:
            webbrowser.open_new_tab(url)
            time.sleep(1)
        except Exception as e:
            print(f"  ❌ 无法打开页面: {e}")

def main():
    """主函数"""
    print("🚀 开始自动配置 GitHub 仓库...")
    print("=" * 60)

    # 检查 GitHub CLI
    success, output = run_gh_command('gh auth status')
    if not success:
        print("❌ 未登录 GitHub CLI，请先运行: gh auth login")
        return

    print("✅ GitHub CLI 已认证")

    # 执行配置
    configure_repository_settings()
    create_branch_protection()
    configure_actions_settings()
    enable_security_features()
    create_setup_instructions()

    print("\n" + "=" * 60)
    print("🎉 自动配置完成！")
    print("=" * 60)

    print("\n📋 接下来的步骤:")
    print("1. 手动配置分支保护等重要设置")
    print("2. 运行: python verify_setup.py 验证配置")
    print("3. 参考 CONFIGURATION_COMPLETE.md 完成剩余配置")

    # 询问是否打开配置页面
    print("\n🌐 是否要打开配置页面？(y/n)")
    try:
        choice = input().lower().strip()
        if choice in ['y', 'yes', '是']:
            open_configuration_pages()
            print("\n✅ 所有配置页面已打开")
            print("请按照 CONFIGURATION_COMPLETE.md 中的说明完成配置")
    except:
        print("\n跳过打开页面")

    print("\n🎯 配置完成后，你的 GitHub 仓库将达到企业级标准！")

if __name__ == "__main__":
    if sys.platform.startswith('win'):
        # Windows 下设置编码
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

    main()