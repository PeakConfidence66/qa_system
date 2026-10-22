"""
CampusPulse 公告采集工具
主程序入口
"""
import sys
import traceback
from PyQt6.QtWidgets import QApplication, QMessageBox
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QFont

from main_window import MainWindow
from config import APP_NAME


def exception_hook(exc_type, exc_value, exc_traceback):
    """全局异常处理钩子"""
    error_msg = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
    print(f"[严重错误] {error_msg}")
    
    # 显示错误对话框
    try:
        QMessageBox.critical(
            None,
            "程序错误",
            f"发生了一个错误:\n\n{exc_value}\n\n请查看控制台获取详细信息。"
        )
    except:
        pass
    
    # 不退出，让程序继续运行
    # sys.exit(1)


def main():
    """主函数"""
    # 设置全局异常处理
    sys.excepthook = exception_hook
    
    # 启用高DPI支持
    if hasattr(Qt, 'AA_EnableHighDpiScaling'):
        QApplication.setAttribute(Qt.ApplicationAttribute.AA_EnableHighDpiScaling, True)
    if hasattr(Qt, 'AA_UseHighDpiPixmaps'):
        QApplication.setAttribute(Qt.ApplicationAttribute.AA_UseHighDpiPixmaps, True)

    # 创建应用
    app = QApplication(sys.argv)
    app.setApplicationName(APP_NAME)

    # 设置全局字体
    font = QFont("Microsoft YaHei", 9)
    app.setFont(font)

    # 设置应用样式
    app.setStyle("Fusion")

    # 创建主窗口
    window = MainWindow()
    window.show()

    # 运行应用
    sys.exit(app.exec())


if __name__ == "__main__":
    main()