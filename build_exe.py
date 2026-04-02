#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
打包脚本 - 使用 PyInstaller 打包局域网文件共享服务器
"""

import os
import sys
import subprocess
import shutil

def main():
    print("=" * 60)
    print("     开始打包局域网文件共享服务器")
    print("=" * 60)
    
    # 检查是否安装了PyInstaller
    try:
        import PyInstaller
        print("✅ PyInstaller 已安装")
    except ImportError:
        print("❌ PyInstaller 未安装，正在安装...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyinstaller"])
        print("✅ PyInstaller 安装完成")
    
    # 打包命令
    script_name = "局域网文件共享服务器.py"
    output_name = "局域网文件共享服务器"
    
    # PyInstaller 参数
    cmd = [
        "pyinstaller",
        "--name", output_name,
        "--onefile",  # 打包成单个exe文件
        "--noconsole",  # 不显示控制台窗口（注释掉此行如果需要看到控制台输出）
        "--icon=NONE",  # 如果有图标可以指定
        "--add-data", "static;static",  # 包含静态资源文件夹
        "--hidden-import", "werkzeug",
        "--hidden-import", "flask",
        "--hidden-import", "waitress",
        "--hidden-import", "configparser",
        script_name
    ]
    
    # 如果需要看到控制台输出，注释掉上面的 --noconsole，改用这个命令
    cmd_with_console = [
        "pyinstaller",
        "--name", output_name,
        "--onefile",
        "--icon=NONE",
        "--add-data", "static;static",
        "--hidden-import", "werkzeug",
        "--hidden-import", "flask",
        "--hidden-import", "waitress",
        "--hidden-import", "configparser",
        script_name
    ]
    
    print(f"\n📦 开始打包: {script_name}")
    print(f"📝 输出名称: {output_name}.exe")
    print("\n⚠️  选择打包模式：")
    print("1. 无控制台窗口（适合日常使用）")
    print("2. 显示控制台窗口（方便查看日志和调试）")
    
    choice = input("\n请选择 (1/2): ").strip()
    
    if choice == "1":
        print("\n📋 使用模式: 无控制台窗口")
        final_cmd = cmd
    else:
        print("\n📋 使用模式: 显示控制台窗口")
        final_cmd = cmd_with_console
    
    print("\n执行命令:")
    print(" ".join(final_cmd))
    print()
    
    try:
        # 执行打包
        subprocess.check_call(final_cmd)
        
        print("\n" + "=" * 60)
        print("✅ 打包成功！")
        print("=" * 60)
        print(f"\n📁 可执行文件位置: dist/{output_name}.exe")
        print("\n📋 使用说明:")
        print("1. 将 dist 文件夹中的 exe 文件复制到任意位置")
        print("2. 首次运行会自动生成 config.ini 配置文件")
        print("3. 编辑 config.ini 修改共享文件夹路径和其他设置")
        print("4. 双击 exe 文件启动服务器")
        print("\n💡 提示:")
        print("- config.ini 必须与 exe 文件在同一目录")
        print("- static 文件夹（如果有）也应该在同一目录")
        print("- 建议创建快捷方式到桌面方便使用")
        
        # 询问是否创建发布包
        print("\n" + "=" * 60)
        create_package = input("是否创建发布包？(y/n): ").strip().lower()
        
        if create_package == 'y':
            create_release_package(output_name)
        
    except subprocess.CalledProcessError as e:
        print(f"\n❌ 打包失败: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 发生错误: {e}")
        sys.exit(1)

def create_release_package(output_name):
    """创建发布包"""
    print("\n📦 正在创建发布包...")
    
    release_dir = "release"
    if os.path.exists(release_dir):
        shutil.rmtree(release_dir)
    os.makedirs(release_dir)
    
    # 复制exe文件
    exe_src = f"dist/{output_name}.exe"
    exe_dst = f"{release_dir}/{output_name}.exe"
    shutil.copy2(exe_src, exe_dst)
    print(f"✅ 已复制: {output_name}.exe")
    
    # 复制配置文件
    if os.path.exists("config.ini"):
        shutil.copy2("config.ini", f"{release_dir}/config.ini")
        print(f"✅ 已复制: config.ini")
    
    # 复制static文件夹（如果存在）
    if os.path.exists("static"):
        shutil.copytree("static", f"{release_dir}/static")
        print(f"✅ 已复制: static 文件夹")
    
    # 创建使用说明
    readme_content = """# 局域网文件共享服务器 使用说明

## 📋 快速开始

1. **首次运行**
   - 双击 `局域网文件共享服务器.exe` 启动程序
   - 程序会自动生成 `config.ini` 配置文件

2. **配置共享路径**
   - 打开 `config.ini` 文件
   - 修改 `UPLOAD_FOLDER` 为您想要的共享文件夹路径
   - 例如: `UPLOAD_FOLDER = E:\\我的共享文件`

3. **启动服务器**
   - 再次双击 `局域网文件共享服务器.exe`
   - 浏览器访问显示的地址（如 http://192.168.1.100:5000）

## ⚙️ 配置说明

### 服务器设置
- `PORT`: 服务器端口（默认5000）
- `ALLOW_LAN`: 是否允许局域网访问（True/False）

### 路径设置
- `UPLOAD_FOLDER`: 共享文件夹路径（存放上传的文件）
- `TEMP_FOLDER`: 临时文件夹（用于分块上传）
- `STATIC_FOLDER`: 静态资源文件夹

### 文件设置
- `MAX_FILE_SIZE_MB`: 单个文件最大大小（MB）
- `MAX_FOLDER_FILES`: 文件夹最多文件数量
- `AUTO_EXTRACT_ZIP`: 是否自动解压ZIP文件
- `ALLOWED_EXTENSIONS`: 允许的文件扩展名（逗号分隔）

## 💡 功能特点

- ✅ 支持超大文件上传（理论无限制）
- ✅ 断点续传（关闭网页后可继续上传）
- ✅ 多用户在线显示
- ✅ 实时活动监控
- ✅ 文件夹分类管理
- ✅ 拖拽移动文件
- ✅ 批量下载（打包成ZIP）

## 🔧 故障排除

1. **无法访问服务器**
   - 检查防火墙是否阻止了端口
   - 确认 `ALLOW_LAN` 设置为 `True`
   - 尝试使用本机地址 http://127.0.0.1:5000

2. **共享文件夹不存在**
   - 程序会自动创建配置中指定的文件夹
   - 确保有足够的磁盘空间

3. **上传失败**
   - 检查共享文件夹是否有写入权限
   - 查看控制台输出的错误信息

## 📞 技术支持

如有问题，请查看：
- 配置文件是否正确
- 控制台是否有错误信息
- 防火墙是否允许程序运行
"""
    
    with open(f"{release_dir}/README.txt", "w", encoding="utf-8") as f:
        f.write(readme_content)
    print(f"✅ 已创建: README.txt")
    
    print("\n" + "=" * 60)
    print(f"✅ 发布包创建成功！")
    print(f"📁 位置: {os.path.abspath(release_dir)}")
    print("=" * 60)
    print("\n可以将整个 release 文件夹打包分发给其他用户。")

if __name__ == "__main__":
    main()

