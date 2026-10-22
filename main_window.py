"""
主窗口模块
"""
import sys


from datetime import datetime
from PyQt6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QComboBox,
    QTreeWidget, QTreeWidgetItem, QTableWidget, QTableWidgetItem,
    QTextEdit, QGroupBox, QRadioButton, QButtonGroup,
    QSplitter, QMenuBar, QMenu, QStatusBar, QMessageBox,
    QHeaderView, QAbstractItemView, QCheckBox, QFileDialog,
    QProgressBar, QDialog
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QAction, QColor, QFont

from config import APP_NAME, TIME_RANGES, TABLE_COLUMNS
from models import Database, School, College, Notice, Link
from dialogs import AddSchoolDialog, DataSourceDialog, EditLinkDialog, AddCollegeDialog
from crawler import CrawlerWorker, ScriptWorker, PreloadWorker, DBWriterWorker
from exporter import ExcelExporter
from batch_import import BatchImportDialog


class MainWindow(QMainWindow):
    """主窗口"""
    
    def __init__(self):
        super().__init__()
        self.setWindowTitle(APP_NAME)
        self.setMinimumSize(1200, 800)
        
        self.db = Database()
        self.crawler_thread = None
        self.db_writer = None  # 数据库写入线程
        self._link_stats = []  # 链接采集统计
        
        self._setup_menu()
        self._setup_ui()
        self._setup_statusbar()
        self._load_schools()
    
    def _setup_menu(self):
        """设置菜单栏"""
        menubar = self.menuBar()
        
        # 文件菜单
        file_menu = menubar.addMenu("文件")
        
        exit_action = QAction("退出", self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        
        # 日志菜单
        log_menu = menubar.addMenu("日志")
        
        clear_log_action = QAction("清空日志", self)
        clear_log_action.triggered.connect(self._clear_log)
        log_menu.addAction(clear_log_action)
        
        # 设置菜单
        settings_menu = menubar.addMenu("设置")
    
    def _setup_ui(self):
        """设置主界面"""
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        main_layout = QHBoxLayout(central_widget)
        main_layout.setContentsMargins(10, 10, 10, 10)
        main_layout.setSpacing(10)
        
        # 分割器
        splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # 左侧面板
        left_panel = self._create_left_panel()
        splitter.addWidget(left_panel)
        
        # 右侧面板
        right_panel = self._create_right_panel()
        splitter.addWidget(right_panel)
        
        # 设置分割比例
        splitter.setSizes([400, 800])
        
        main_layout.addWidget(splitter)
    
    def _create_left_panel(self):
        """创建左侧面板"""
        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)
        
        # 搜索框
        search_layout = QHBoxLayout()
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("搜索学校/学院/链接（空格分隔，匹配任意一个）")
        self.search_input.textChanged.connect(self._filter_schools)
        
        clear_btn = QPushButton("清空")
        clear_btn.setFixedWidth(60)
        clear_btn.clicked.connect(lambda: self.search_input.clear())
        
        search_layout.addWidget(self.search_input)
        search_layout.addWidget(clear_btn)
        layout.addLayout(search_layout)
        
        # 数据源标签
        source_label = QLabel("数据源 (学校/学院/链接)")
        source_label.setStyleSheet("font-weight: bold; color: #666;")
        layout.addWidget(source_label)
        
        # 树形控件
        self.tree = QTreeWidget()
        self.tree.setHeaderHidden(True)
        self.tree.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.tree.itemClicked.connect(self._on_tree_item_clicked)
        self.tree.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.tree.customContextMenuRequested.connect(self._show_tree_context_menu)
        layout.addWidget(self.tree)
        
        # 统计信息
        stats_layout = QHBoxLayout()
        self.school_count_label = QLabel("学校: 0")
        self.college_count_label = QLabel("学院: 0")
        self.link_count_label = QLabel("链接: 0")
        
        stats_layout.addWidget(self.school_count_label)
        stats_layout.addWidget(self.college_count_label)
        stats_layout.addWidget(self.link_count_label)
        stats_layout.addStretch()
        
        # 统计副链接复选框
        self.count_sub_links_cb = QCheckBox("统计副链接")
        self.count_sub_links_cb.setChecked(True)
        self.count_sub_links_cb.stateChanged.connect(self._load_schools)
        stats_layout.addWidget(self.count_sub_links_cb)
        
        layout.addLayout(stats_layout)
        
        # 底部按钮
        btn_layout = QHBoxLayout()
        
        self.add_school_btn = QPushButton("添加学校")
        self.add_school_btn.setStyleSheet(self._get_blue_button_style())
        self.add_school_btn.clicked.connect(self._add_school)
        
        self.add_college_btn = QPushButton("添加学院")
        self.add_college_btn.setStyleSheet(self._get_blue_button_style())
        self.add_college_btn.clicked.connect(self._add_college)
        
        self.add_source_btn = QPushButton("添加链接源")
        self.add_source_btn.setStyleSheet(self._get_blue_button_style())
        self.add_source_btn.clicked.connect(self._add_data_source)
        
        self.batch_import_btn = QPushButton("批量导入")
        self.batch_import_btn.setStyleSheet(self._get_gray_button_style())
        self.batch_import_btn.clicked.connect(self._batch_import)
        
        self.edit_rule_btn = QPushButton("编辑规则")
        self.edit_rule_btn.setStyleSheet(self._get_gray_button_style())
        self.edit_rule_btn.clicked.connect(self._edit_data_source)
        
        self.delete_btn = QPushButton("删除")
        self.delete_btn.setStyleSheet(self._get_gray_button_style())
        self.delete_btn.clicked.connect(self._delete_item)
        
        btn_layout.addWidget(self.add_school_btn)
        btn_layout.addWidget(self.add_college_btn)
        btn_layout.addWidget(self.add_source_btn)
        btn_layout.addWidget(self.batch_import_btn)
        btn_layout.addWidget(self.edit_rule_btn)
        btn_layout.addWidget(self.delete_btn)
        
        layout.addLayout(btn_layout)
        
        return panel
    
    def _create_right_panel(self):
        """创建右侧面板"""
        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)
        
        # 采集控制组
        control_group = QGroupBox("采集控制")
        control_layout = QVBoxLayout(control_group)
        
        # 范围选择
        range_layout = QHBoxLayout()
        range_label = QLabel("范围:")
        
        self.range_group = QButtonGroup(self)
        self.range_selected = QRadioButton("选中目标")
        self.range_school = QRadioButton("单个学校")
        self.range_all = QRadioButton("全部")
        self.range_all.setChecked(True)
        
        self.range_group.addButton(self.range_selected)
        self.range_group.addButton(self.range_school)
        self.range_group.addButton(self.range_all)
        
        range_layout.addWidget(range_label)
        range_layout.addWidget(self.range_selected)
        range_layout.addWidget(self.range_school)
        range_layout.addWidget(self.range_all)
        range_layout.addStretch()
        
        self.range_status_label = QLabel("未选择，默认: 全部")
        range_layout.addWidget(self.range_status_label)
        
        control_layout.addLayout(range_layout)
        
        # 时间和关键词
        filter_layout = QHBoxLayout()
        
        time_label = QLabel("时间:")
        self.time_combo = QComboBox()
        self.time_combo.addItems(list(TIME_RANGES.keys()))
        self.time_combo.setCurrentText("7天内")
        
        include_label = QLabel("包含关键词:")
        self.include_input = QLineEdit()
        self.include_input.setPlaceholderText("逗号分隔，例如: 复试,调剂...")
        
        exclude_label = QLabel("排除关键词:")
        self.exclude_input = QLineEdit()
        self.exclude_input.setPlaceholderText("逗号分隔，例如: 补...")
        
        threads_label = QLabel("线程数:")
        self.threads_combo = QComboBox()
        self.threads_combo.addItems(["1", "2", "3", "4", "5", "6", "8", "10","15","20"])
        self.threads_combo.setCurrentText("3")
        self.threads_combo.setFixedWidth(50)
        
        filter_layout.addWidget(time_label)
        filter_layout.addWidget(self.time_combo)
        filter_layout.addWidget(include_label)
        filter_layout.addWidget(self.include_input)
        filter_layout.addWidget(exclude_label)
        filter_layout.addWidget(self.exclude_input)
        filter_layout.addWidget(threads_label)
        filter_layout.addWidget(self.threads_combo)
        
        control_layout.addLayout(filter_layout)
        
        # 进度条
        progress_layout = QHBoxLayout()
        progress_label = QLabel("进度:")
        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        
        progress_layout.addWidget(progress_label)
        progress_layout.addWidget(self.progress_bar)
        control_layout.addLayout(progress_layout)
        
        # 控制按钮
        btn_layout = QHBoxLayout()
        
        self.start_btn = QPushButton("开始采集")
        self.start_btn.setStyleSheet(self._get_blue_button_style())
        self.start_btn.clicked.connect(self._start_crawl)
        
        self.stop_btn = QPushButton("停止采集")
        self.stop_btn.setStyleSheet(self._get_gray_button_style())
        self.stop_btn.clicked.connect(self._stop_crawl)
        self.stop_btn.setEnabled(False)
        
        btn_layout.addWidget(self.start_btn)
        btn_layout.addWidget(self.stop_btn)
        btn_layout.addStretch()
        
        self.export_btn = QPushButton("导出Excel")
        self.export_btn.setStyleSheet(self._get_gray_button_style())
        self.export_btn.clicked.connect(self._export_excel)
        btn_layout.addWidget(self.export_btn)
        
        self.export_log_btn = QPushButton("导出日志")
        self.export_log_btn.setStyleSheet(self._get_gray_button_style())
        self.export_log_btn.clicked.connect(self._export_log)
        btn_layout.addWidget(self.export_log_btn)
        
        self.export_stats_btn = QPushButton("导出统计")
        self.export_stats_btn.setStyleSheet(self._get_gray_button_style())
        self.export_stats_btn.clicked.connect(self._export_stats)
        btn_layout.addWidget(self.export_stats_btn)
        
        self.run_script_btn = QPushButton("执行脚本")
        self.run_script_btn.setStyleSheet(self._get_gray_button_style())
        self.run_script_btn.clicked.connect(self._run_custom_scripts)
        btn_layout.addWidget(self.run_script_btn)
        
        control_layout.addLayout(btn_layout)
        
        layout.addWidget(control_group)
        
        # 日志区域
        log_group = QGroupBox("日志")
        log_layout = QVBoxLayout(log_group)
        
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setMaximumHeight(120)
        log_layout.addWidget(self.log_text)
        
        layout.addWidget(log_group)
        
        # 数据预览表格
        preview_label = QLabel("数据预览 (当前 0 条)")
        preview_label.setStyleSheet("font-weight: bold;")
        layout.addWidget(preview_label)
        self.preview_label = preview_label
        
        self.table = QTableWidget()
        self.table.setColumnCount(len(TABLE_COLUMNS))
        self.table.setHorizontalHeaderLabels(TABLE_COLUMNS)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        # 链接列点击打开浏览器
        self.table.cellClicked.connect(self._on_table_cell_clicked)
        layout.addWidget(self.table)
        
        # 分页控件
        self.page_size = 100  # 每页条数
        self.current_page = 1
        self.total_pages = 1
        self.total_count = 0
        
        page_layout = QHBoxLayout()
        page_layout.addStretch()
        
        self.prev_btn = QPushButton("上一页")
        self.prev_btn.setStyleSheet(self._get_gray_button_style())
        self.prev_btn.clicked.connect(self._prev_page)
        self.prev_btn.setEnabled(False)
        page_layout.addWidget(self.prev_btn)
        
        self.page_label = QLabel("第 1 / 1 页")
        page_layout.addWidget(self.page_label)
        
        self.next_btn = QPushButton("下一页")
        self.next_btn.setStyleSheet(self._get_gray_button_style())
        self.next_btn.clicked.connect(self._next_page)
        self.next_btn.setEnabled(False)
        page_layout.addWidget(self.next_btn)
        
        layout.addLayout(page_layout)
        
        return panel
    
    def _setup_statusbar(self):
        """设置状态栏"""
        self.statusbar = QStatusBar()
        self.setStatusBar(self.statusbar)
        self.statusbar.showMessage("就绪")
    
    def _get_blue_button_style(self):
        """蓝色按钮样式"""
        return """
            QPushButton {
                background-color: #1890ff;
                color: white;
                border: none;
                padding: 6px 15px;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #40a9ff;
            }
            QPushButton:disabled {
                background-color: #bae7ff;
            }
        """
    
    def _get_gray_button_style(self):
        """灰色按钮样式"""
        return """
            QPushButton {
                background-color: #f0f0f0;
                border: 1px solid #d9d9d9;
                padding: 6px 15px;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #e6e6e6;
            }
        """
    
    def _load_schools(self):
        """加载学校列表"""
        self.tree.clear()
        schools = self.db.get_schools()
        
        count_sub = self.count_sub_links_cb.isChecked()
        
        school_count = len(schools)
        college_count = 0
        link_count = 0
        
        for school in schools:
            # 学校节点
            school_item = QTreeWidgetItem(self.tree)
            school_item.setText(0, f"▶ {school.name}")
            school_item.setData(0, Qt.ItemDataRole.UserRole, {"type": "school", "id": school.id})
            
            # 加载学院
            colleges = self.db.get_colleges_by_school(school.id)
            for college in colleges:
                college_item = QTreeWidgetItem(school_item)
                college_item.setText(0, f"  ▼ {college.name}")
                college_item.setData(0, Qt.ItemDataRole.UserRole, {"type": "college", "id": college.id})
                college_count += 1
                
                # 加载该学院的链接，分主链接和副链接
                links = self.db.get_links_by_college(college.id)
                for link in links:
                    link_item = QTreeWidgetItem(college_item)
                    # 根据链接类型显示不同前缀
                    if link.link_type == 'main':
                        prefix = "📄 主："
                    else:
                        prefix = "📑 副："
                    link_item.setText(0, f"    {prefix}{link.url}")
                    link_item.setData(0, Qt.ItemDataRole.UserRole, {"type": "link", "id": link.id})
                    link_item.setForeground(0, Qt.GlobalColor.blue)
                    # 根据复选框决定是否统计副链接
                    if count_sub or link.link_type == 'main':
                        link_count += 1
        
        # 更新统计
        self.school_count_label.setText(f"学校: {school_count}")
        self.college_count_label.setText(f"学院: {college_count}")
        self.link_count_label.setText(f"链接: {link_count}")
        
        self._log(f"[信息] 已加载 {school_count} 所学校, {college_count} 个学院, {link_count} 个链接")
    
    def _filter_schools(self):
        """过滤学校列表"""
        search_text = self.search_input.text().lower().strip()
        # 支持空格分隔的多关键词搜索
        keywords = [k for k in search_text.split() if k]
        
        for i in range(self.tree.topLevelItemCount()):
            item = self.tree.topLevelItem(i)
            self._filter_tree_item(item, keywords)
    
    def _filter_tree_item(self, item, keywords, parent_matched=False):
        """递归过滤树节点，支持多关键词（任意匹配即可）
        
        Args:
            item: 当前节点
            keywords: 关键词列表
            parent_matched: 父节点是否已匹配（如果父节点匹配，子节点应该显示）
        """
        text = item.text(0).lower()
        # 任意一个关键词匹配即可（OR逻辑）
        self_match = any(k in text for k in keywords) if keywords else True
        
        # 如果父节点已匹配，当前节点也应该显示
        should_show = self_match or parent_matched
        
        # 检查子节点
        child_match = False
        for i in range(item.childCount()):
            child = item.child(i)
            if self._filter_tree_item(child, keywords, should_show):
                child_match = True
        
        # 当前节点可见：自身匹配 或 父节点匹配 或 有子节点匹配
        visible = should_show or child_match
        item.setHidden(not visible)
        return visible
    
    def _on_table_cell_clicked(self, row, column):
        """表格单元格点击事件 - 点击链接列打开浏览器"""
        if column == 1:  # 链接列
            item = self.table.item(row, column)
            if item:
                url = item.data(Qt.ItemDataRole.UserRole)
                if url:
                    import webbrowser
                    webbrowser.open(url)
                    self._log(f"[信息] 打开链接: {url}")
    
    def _on_tree_item_clicked(self, item, column):
        """树节点点击事件"""
        data = item.data(0, Qt.ItemDataRole.UserRole)
        if data:
            item_type = data.get("type")
            item_id = data.get("id")
            
            if item_type == "school":
                self.range_status_label.setText(f"已选择学校 ID: {item_id}")
            elif item_type == "source":
                self.range_status_label.setText(f"已选择数据源 ID: {item_id}")
            elif item_type == "college":
                self.range_status_label.setText(f"已选择学院 ID: {item_id}")
    
    def _show_tree_context_menu(self, position):
        """显示树右键菜单"""
        # 可扩展右键菜单功能
        pass
    
    def _add_school(self):
        """添加学校"""
        # 第一步：输入学校名称
        dialog = AddSchoolDialog(self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            school_name = dialog.get_school_name()
            
            # 第二步：自动弹出数据源配置对话框
            ds_dialog = DataSourceDialog(school_name, self)
            if ds_dialog.exec() == QDialog.DialogCode.Accepted:
                self._load_schools()
                self._log(f"[信息] 添加学校成功: {school_name}")
    
    def _batch_import(self):
        """批量导入"""
        dialog = BatchImportDialog(self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            self._load_schools()
            self._log("[信息] 批量导入完成")
    
    def _add_college(self):
        """添加学院"""
        # 获取选中的学校
        selected = self.tree.selectedItems()
        if not selected:
            QMessageBox.warning(self, "警告", "请先选择一所学校")
            return
        
        data = selected[0].data(0, Qt.ItemDataRole.UserRole)
        if not data or data.get("type") != "school":
            QMessageBox.warning(self, "警告", "请先选择一所学校")
            return
        
        school_id = data.get("id")
        dialog = AddCollegeDialog(school_id, self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            self._load_schools()
            self._log(f"[信息] 添加学院成功: {dialog.get_college_name()}")
    
    def _add_data_source(self):
        """添加链接源"""
        selected = self.tree.selectedItems()
        if not selected:
            QMessageBox.warning(self, "警告", "请先选择一个学院")
            return
        
        data = selected[0].data(0, Qt.ItemDataRole.UserRole)
        if not data or data.get("type") != "college":
            QMessageBox.warning(self, "警告", "请先选择一个学院")
            return
        
        college_id = data.get("id")
        
        from PyQt6.QtWidgets import QInputDialog
        url, ok = QInputDialog.getText(self, "添加链接", "请输入链接URL:")
        if ok and url.strip():
            link = Link(college_id=college_id, url=url.strip(), link_type='main')
            link.set_rules({
                "list_selector": "",
                "title_selector": "",
                "link_selector": "",
                "date_selector": "",
                "date_format": "%Y-%m-%d"
            })
            self.db.add_link(link)
            self._load_schools()
            self._log(f"[信息] 添加链接成功: {url.strip()}")
    
    def _edit_data_source(self):
        """编辑链接规则"""
        selected = self.tree.selectedItems()
        if not selected:
            QMessageBox.warning(self, "警告", "请先选择一个链接")
            return
        
        data = selected[0].data(0, Qt.ItemDataRole.UserRole)
        if not data or data.get("type") != "link":
            QMessageBox.warning(self, "警告", "请先选择一个链接")
            return
        
        link_id = data.get("id")
        dialog = EditLinkDialog(link_id=link_id, parent=self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            self._load_schools()
            self._log("[信息] 更新链接规则成功")
    
    def _delete_item(self):
        """删除选中项（支持批量删除）"""
        selected = self.tree.selectedItems()
        if not selected:
            QMessageBox.information(self, "提示", "请先选择要删除的项目")
            return
        
        type_names = {
            "school": "学校",
            "college": "学院",
            "link": "链接"
        }
        
        if len(selected) == 1:
            # 单个删除
            item = selected[0]
            data = item.data(0, Qt.ItemDataRole.UserRole)
            if not data:
                return
            
            item_type = data.get("type")
            item_id = data.get("id")
            item_name = item.text(0).strip("▶▼📄📑 ")
            type_name = type_names.get(item_type, "项目")
            
            if item_type == "school":
                tip = f"将同时删除该学校下的所有学院和链接！"
            elif item_type == "college":
                tip = f"将同时删除该学院下的所有链接！"
            else:
                tip = ""
            
            reply = QMessageBox.question(
                self, "确认删除",
                f"确定要删除{type_name}「{item_name}」吗？\n{tip}",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No
            )
            
            if reply == QMessageBox.StandardButton.Yes:
                try:
                    if item_type == "school":
                        self.db.delete_school(item_id)
                    elif item_type == "college":
                        self.db.delete_college(item_id)
                    elif item_type == "link":
                        self.db.delete_link(item_id)
                    
                    self._load_schools()
                    self._log(f"[信息] 已删除{type_name}: {item_name}")
                except Exception as e:
                    QMessageBox.critical(self, "错误", f"删除失败: {str(e)}")
        else:
            # 批量删除
            # 统计要删除的项目
            items_info = []
            for item in selected:
                data = item.data(0, Qt.ItemDataRole.UserRole)
                if not data:
                    continue
                item_type = data.get("type")
                item_id = data.get("id")
                item_name = item.text(0).strip("▶▼📄📑 ")
                type_name = type_names.get(item_type, "项目")
                items_info.append({
                    "type": item_type,
                    "id": item_id,
                    "name": item_name,
                    "type_name": type_name
                })
            
            if not items_info:
                return
            
            # 生成摘要
            school_count = sum(1 for i in items_info if i["type"] == "school")
            college_count = sum(1 for i in items_info if i["type"] == "college")
            link_count = sum(1 for i in items_info if i["type"] == "link")
            
            parts = []
            if school_count > 0:
                parts.append(f"{school_count} 所学校（含其下所有学院和链接）")
            if college_count > 0:
                parts.append(f"{college_count} 个学院（含其下所有链接）")
            if link_count > 0:
                parts.append(f"{link_count} 个链接")
            
            summary = "、".join(parts)
            
            reply = QMessageBox.question(
                self, "批量删除",
                f"确定要删除选中的 {len(items_info)} 个项目吗？\n\n{summary}",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No
            )
            
            if reply == QMessageBox.StandardButton.Yes:
                try:
                    deleted_schools = 0
                    deleted_colleges = 0
                    deleted_links = 0
                    
                    for info in items_info:
                        if info["type"] == "school":
                            self.db.delete_school(info["id"])
                            deleted_schools += 1
                        elif info["type"] == "college":
                            self.db.delete_college(info["id"])
                            deleted_colleges += 1
                        elif info["type"] == "link":
                            self.db.delete_link(info["id"])
                            deleted_links += 1
                    
                    self._load_schools()
                    self._log(f"[信息] 批量删除完成: {deleted_schools}所学校, {deleted_colleges}个学院, {deleted_links}个链接")
                except Exception as e:
                    QMessageBox.critical(self, "错误", f"删除失败: {str(e)}")
    
    def _start_crawl(self):
        """开始采集（使用预加载线程避免UI卡顿）"""
        # 获取采集参数
        time_range = self.time_combo.currentText()
        days = TIME_RANGES.get(time_range, 7)
        
        include_keywords = self.include_input.text().strip()
        exclude_keywords = self.exclude_input.text().strip()
        
        self._log(f"[配置] 包含关键词: '{include_keywords}'")
        self._log(f"[配置] 排除关键词: '{exclude_keywords}'")
        
        # 获取线程数
        max_workers = int(self.threads_combo.currentText())
        
        # 获取采集范围和选中的项目
        if self.range_all.isChecked():
            range_mode = "all"
            selected_items = []
        elif self.range_selected.isChecked():
            range_mode = "selected"
            selected_items = self._get_selected_item_data()
        else:  # range_school
            range_mode = "school"
            selected_items = self._get_selected_item_data()
        
        # 禁用按钮
        self.start_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.statusbar.showMessage("准备数据...")
        self._log("[信息] 正在准备数据，请稍候...")
        
        # 保存参数到实例变量
        self._pending_crawl = {
            'days': days,
            'include_keywords': include_keywords,
            'exclude_keywords': exclude_keywords,
            'max_workers': max_workers,
            'range_mode': range_mode
        }
        
        # 创建预加载线程
        self._preload_thread = PreloadWorker(self.db, range_mode, selected_items)
        self._preload_thread.progress.connect(self._on_preload_progress)
        self._preload_thread.finished.connect(self._on_preload_finished)
        self._preload_thread.start()
    
    def _get_selected_item_data(self):
        """获取选中的项目数据"""
        selected = self.tree.selectedItems()
        items = []
        for item in selected:
            data = item.data(0, Qt.ItemDataRole.UserRole)
            if data:
                items.append(data)
        return items
    
    def _on_preload_progress(self, message):
        """预加载进度"""
        self.statusbar.showMessage(message)
        self._log(message)
    
    def _on_preload_finished(self, sources, selected_items):
        """预加载完成，启动爬虫"""
        if not sources:
            QMessageBox.warning(self, "警告", "没有可采集的数据源")
            self.start_btn.setEnabled(True)
            self.stop_btn.setEnabled(False)
            self.statusbar.showMessage("准备就绪")
            return
        
        params = self._pending_crawl
        self._log(f"[信息] 获取到 {len(sources)} 个链接，开始采集...")
        
        # 启动数据库写入线程（完全在后台处理）
        self.db_writer = DBWriterWorker()
        self.db_writer.log.connect(self._log)
        self.db_writer.finished.connect(self._on_db_write_finished)
        self.db_writer.start()
        
        # 启动爬虫线程
        self.crawler_thread = CrawlerWorker(
            sources, params['days'], params['include_keywords'], 
            params['exclude_keywords'], params['max_workers']
        )
        self.crawler_thread.progress.connect(self._on_crawl_progress)
        self.crawler_thread.log.connect(self._on_crawl_log)
        self.crawler_thread.result_summary.connect(self._on_crawl_result)
        self.crawler_thread.notice_found.connect(self._on_notice_found_to_writer)
        self.crawler_thread.finished.connect(self._on_crawl_finished)
        self.crawler_thread.start()
        
        self.statusbar.showMessage("采集中...")
        self._log("[信息] 开始采集...")
    
    def _on_notice_found_to_writer(self, notice_data):
        """直接将公告数据发送到写入线程队列（零开销）"""
        self.db_writer.add_notices([notice_data])
    
    def _on_db_write_finished(self):
        """数据库写入完成"""
        self._log("[信息] 数据库写入完成")
    
    def _stop_crawl(self):
        """停止采集"""
        if self.crawler_thread and self.crawler_thread.isRunning():
            self.crawler_thread.stop()
            self.crawler_thread.wait()
        
        # 停止数据库写入线程
        if self.db_writer and self.db_writer.isRunning():
            self.db_writer.stop()
            self.db_writer.wait()
        
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.statusbar.showMessage("已停止")
        self._log("[信息] 采集已停止")
    
    def _on_crawl_log(self, message):
        """爬虫日志（限制频率，避免UI卡顿）"""
        self._log(message)
    
    def _on_crawl_progress(self, current, total):
        """采集进度更新"""
        progress = int(current / total * 100) if total > 0 else 0
        self.progress_bar.setValue(progress)
        self.statusbar.showMessage(f"采集中... {current}/{total}")
    
    def _on_crawl_result(self, summary):
        """接收采集结果汇总"""
        self._crawl_summary = summary
        self._link_stats = summary.get("link_stats", [])  # 保存链接统计
    
    def _on_crawl_finished(self):
        """采集完成"""
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.progress_bar.setValue(100)
        self.statusbar.showMessage("采集完成")
        
        # 等待数据库写入线程完成
        if self.db_writer and self.db_writer.isRunning():
            self._log("[信息] 等待数据写入完成...")
            self.db_writer.stop()  # 发送停止信号
            self.db_writer.wait()  # 等待完成
            self._log("[信息] 数据写入完成")
        
        try:
            # 重置到第一页并刷新表格
            self.current_page = 1
            self._load_table()
            
            # 生成统计报告
            stats_report = self._generate_stats_report()
            
            # 弹出结果汇总
            summary = getattr(self, '_crawl_summary', {})
            total = summary.get("total_after_filter", 0)
            days = summary.get("days", 7)
            include_kw = summary.get("include_keywords", "(无)")
            exclude_kw = summary.get("exclude_keywords", "(无)")
            link_stats = getattr(self, '_link_stats', [])
            
            # 统计链接采集情况
            success_count = sum(1 for s in link_stats if s.get("success"))
            fail_count = sum(1 for s in link_stats if not s.get("success"))
            warning_count = sum(1 for s in link_stats if s.get("has_warning"))
            zero_count = sum(1 for s in link_stats if s.get("count", 0) == 0 and not s.get("has_warning"))
            
            # 获取前8条预览
            notices = self.db.get_notices(limit=8)
            preview_lines = []
            for n in notices:
                title_preview = n.title[:35] if len(n.title) > 35 else n.title
                url_preview = n.url[:50] if n.url else "(无链接)"
                preview_lines.append(f"  • {title_preview}")
                preview_lines.append(f"    链接: {url_preview}")
            preview_text = "\n".join(preview_lines) if preview_lines else "(无)"
            
            msg = (
                f"结果汇总:\n"
                f"  总条数: {total}\n"
                f"  时间范围: {days} 天内\n"
                f"  包含关键词: {include_kw}\n"
                f"  排除关键词: {exclude_kw}\n\n"
                f"链接采集统计:\n"
                f"  总链接数: {len(link_stats)}\n"
                f"  采集成功: {success_count}\n"
                f"  采集失败: {fail_count}\n"
                f"  有异常状态码: {warning_count}\n"
                f"  抓取为0: {zero_count}\n\n"
                f"预览 (前 {min(8, total)} 条):\n"
                f"{preview_text}"
            )
            
            QMessageBox.information(self, "采集完成", msg)
            self._log(stats_report)  # 在日志区域也显示统计报告
            
        except Exception as e:
            self._log(f"[错误] 处理结果时出错: {str(e)}")
            import traceback
            traceback.print_exc()
    
    def _generate_stats_report(self):
        """生成统计报告 - 基于数据库实际入库数据"""
        link_stats = getattr(self, '_link_stats', [])
        if not link_stats:
            return "[统计] 无链接统计数据"
        
        # 从数据库查询每个(学校, 学院)组合的实际入库数量
        db_count_school_college = self.db.get_count_by_school_college()
        
        # 为每条 link_stats 赋实际入库数量
        for s in link_stats:
            school = s.get('school', '')
            college = s.get('college', '')
            url = s.get('url', '')
            # 按学校+学院统计
            cnt = db_count_school_college.get(school, {}).get(college, 0)
            s['db_count'] = cnt
            s['url_for_match'] = url  # 保存URL用于报告
        
        success = [s for s in link_stats if s.get("success")]
        failed = [s for s in link_stats if not s.get("success")]
        has_warning = [s for s in success if s.get("has_warning")]
        # 抓取0条：当前链接的入库数量为0（但成功）
        zero = [s for s in success if s.get("db_count", 0) == 0 and not s.get("has_warning")]
        
        # 按学校和学院分组
        school_groups = {}
        for s in link_stats:
            school = s.get('school', '未知')
            college = s.get('college', '未知')
            if school not in school_groups:
                school_groups[school] = {}
            if college not in school_groups[school]:
                school_groups[school][college] = []
            school_groups[school][college].append(s)
        
        lines = []
        lines.append("=" * 60)
        lines.append("采集统计报告 (基于数据库实际数据)")
        lines.append("=" * 60)
        lines.append(f"总链接数: {len(link_stats)}")
        lines.append(f"采集成功(无异常): {sum(1 for s in success if not s.get('has_warning'))}")
        lines.append(f"采集成功(有异常状态码): {len(has_warning)}")
        lines.append(f"采集失败(异常): {len(failed)}")
        lines.append(f"成功但入库为0: {len(zero)}")
        lines.append(f"数据库实际入库总数: {sum(s.get('db_count', 0) for s in link_stats)}")
        lines.append("")
        
        # 按学校和学院分层显示
        lines.append("=" * 60)
        lines.append("【分层统计 - 按学校/学院/链接】")
        lines.append("=" * 60)
        for school, colleges in school_groups.items():
            school_total = sum(s.get('db_count', 0) for cl in colleges.values() for s in cl)
            lines.append(f"\n▼ {school} (入库 {school_total} 条)")
            for college, stats in colleges.items():
                college_total = sum(s.get('db_count', 0) for s in stats)
                lines.append(f"  ├─ {college} (入库 {college_total} 条)")
                for s in stats:
                    url = s.get('url', '未知')
                    db_cnt = s.get('db_count', 0)
                    parsed_cnt = s.get('count', 0)
                    if db_cnt > 0:
                        lines.append(f"  │  ├─ ✓ {url[:70]}")
                        lines.append(f"  │  │  └─ 入库 {db_cnt} 条" + (f" (解析{parsed_cnt}条)" if parsed_cnt != db_cnt else ""))
                    elif s.get('has_warning'):
                        lines.append(f"  │  ├─ ⚠ {url[:70]}")
                        lines.append(f"  │  │  └─ 入库 0 条，状态码异常")
                    elif not s.get('success'):
                        lines.append(f"  │  ├─ ✗ {url[:70]}")
                        lines.append(f"  │  │  └─ 采集失败: {s.get('error', '未知')[:60]}")
                    else:
                        lines.append(f"  │  ├─ ○ {url[:70]}")
                        lines.append(f"  │  │  └─ 入库 0 条 (可能URL已存在或关键词不匹配)")
        
        # 详细信息
        if failed:
            lines.append("\n" + "=" * 60)
            lines.append("【采集失败的链接】")
            lines.append("=" * 60)
            for s in failed:
                lines.append(f"  学校: {s.get('school', '未知')}")
                lines.append(f"  部门: {s.get('college', '未知')}")
                lines.append(f"  URL: {s.get('url', '未知')}")
                lines.append(f"  错误: {s.get('error', '未知错误')}")
                lines.append("")
        
        if has_warning:
            lines.append("=" * 60)
            lines.append("【有异常状态码的链接】")
            lines.append("=" * 60)
            for s in has_warning:
                lines.append(f"  学校: {s.get('school', '未知')}")
                lines.append(f"  部门: {s.get('college', '未知')}")
                lines.append(f"  URL: {s.get('url', '未知')}")
                lines.append(f"  入库条数: {s.get('db_count', 0)}")
                warnings = s.get('warnings', [])
                if warnings:
                    lines.append(f"  ⚠ 问题: {', '.join(warnings)}")
                lines.append("")
        
        if zero:
            lines.append("=" * 60)
            lines.append("【成功但入库为0的链接】")
            lines.append("=" * 60)
            for s in zero:
                lines.append(f"  {s.get('school', '未知')} / {s.get('college', '未知')}")
                lines.append(f"    URL: {s.get('url', '未知')}")
                lines.append(f"    原因: 可能是URL已存在、关键词不匹配或日期超出范围")
        
        lines.append("\n" + "=" * 60)
        return "\n".join(lines)
    
    def _export_stats(self):
        """导出统计报告"""
        stats_report = self._generate_stats_report()
        
        if not getattr(self, '_link_stats', []):
            QMessageBox.warning(self, "警告", "暂无统计数据")
            return
        
        default_name = f"采集统计_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        file_path, _ = QFileDialog.getSaveFileName(
            self, "导出统计报告", default_name, "文本文件 (*.txt)"
        )
        
        if file_path:
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(stats_report)
                QMessageBox.information(self, "成功", f"已导出到: {file_path}")
                self._log(f"[信息] 导出统计报告成功: {file_path}")
            except Exception as e:
                QMessageBox.critical(self, "错误", f"导出失败: {str(e)}")
    
    def _get_selected_sources(self):
        """根据范围选择获取链接列表（支持多选）"""
        all_links = self.db.get_all_links()
        
        if self.range_all.isChecked():
            # 全部
            return all_links
        
        selected = self.tree.selectedItems()
        if not selected:
            return all_links
        
        if self.range_selected.isChecked():
            # 选中目标 - 支持多选
            result_links = []
            for item in selected:
                data = item.data(0, Qt.ItemDataRole.UserRole)
                if not data:
                    continue
                
                item_type = data.get("type")
                item_id = data.get("id")
                
                if item_type == "link":
                    # 选中链接
                    link = next((l for l in all_links if l['id'] == item_id), None)
                    if link and link not in result_links:
                        result_links.append(link)
                elif item_type == "college":
                    # 选中学院 - 获取该学院下所有链接
                    college_links = [l for l in all_links if l.get('college_id') == item_id]
                    for link in college_links:
                        if link not in result_links:
                            result_links.append(link)
                elif item_type == "school":
                    # 选中学校 - 获取该学校下所有链接
                    school_links = [l for l in all_links if l.get('school_id') == item_id]
                    for link in school_links:
                        if link not in result_links:
                            result_links.append(link)
            
            self._log(f"[信息] 选中目标: {len(selected)} 个项, 共 {len(result_links)} 个链接")
            return result_links
        
        elif self.range_school.isChecked():
            # 单个学校 - 只取第一个选中的学校
            item = selected[0]
            data = item.data(0, Qt.ItemDataRole.UserRole)
            if not data:
                return all_links
            
            item_type = data.get("type")
            item_id = data.get("id")
            
            school_id = item_id
            if item_type == "college":
                school_id = item.parent().data(0, Qt.ItemDataRole.UserRole).get("id") if item.parent() else None
            elif item_type == "link":
                parent = item.parent()
                if parent:
                    grandparent = parent.parent()
                    if grandparent:
                        school_id = grandparent.data(0, Qt.ItemDataRole.UserRole).get("id")
            
            if school_id:
                return [l for l in all_links if l.get('school_id') == school_id]
        
        return all_links
    
    def _load_table(self):
        """加载表格数据（支持分页）"""
        # 获取总数
        self.total_count = self.db.get_notices_count()
        self.total_pages = (self.total_count + self.page_size - 1) // self.page_size if self.total_count > 0 else 1
        
        # 确保当前页有效
        if self.current_page > self.total_pages:
            self.current_page = self.total_pages
        if self.current_page < 1:
            self.current_page = 1
        
        # 计算偏移量
        offset = (self.current_page - 1) * self.page_size
        
        # 获取当前页数据
        notices = self.db.get_notices(limit=self.page_size, offset=offset)
        self.table.setRowCount(len(notices))
        
        for i, notice in enumerate(notices):
            self.table.setItem(i, 0, QTableWidgetItem(notice.title))
            self.table.setItem(i, 2, QTableWidgetItem(notice.publish_date))
            self.table.setItem(i, 3, QTableWidgetItem(notice.school))
            self.table.setItem(i, 4, QTableWidgetItem(notice.category))
            self.table.setItem(i, 5, QTableWidgetItem(notice.keywords))
            self.table.setItem(i, 6, QTableWidgetItem(str(notice.days_old)))
            
            # 链接列设置为蓝色带下划线，并存储URL到data中
            link_item = QTableWidgetItem(notice.url)
            link_item.setForeground(QColor("#0563C1"))
            link_item.setFont(QFont("Microsoft YaHei", 9, QFont.Weight.Normal, True))  # 下划线
            link_item.setData(Qt.ItemDataRole.UserRole, notice.url)  # 存储URL
            link_item.setToolTip(f"点击打开: {notice.url}")
            self.table.setItem(i, 1, link_item)
        
        # 更新分页控件
        self._update_pagination()
        
        start_num = offset + 1 if notices else 0
        end_num = offset + len(notices)
        self.preview_label.setText(f"数据预览 (共 {self.total_count} 条，显示 {start_num}-{end_num})")
    
    def _update_pagination(self):
        """更新分页控件状态"""
        self.page_label.setText(f"第 {self.current_page} / {self.total_pages} 页")
        self.prev_btn.setEnabled(self.current_page > 1)
        self.next_btn.setEnabled(self.current_page < self.total_pages)
    
    def _prev_page(self):
        """上一页"""
        if self.current_page > 1:
            self.current_page -= 1
            self._load_table()
    
    def _next_page(self):
        """下一页"""
        if self.current_page < self.total_pages:
            self.current_page += 1
            self._load_table()
    
    def _export_excel(self):
        """导出Excel（导出全部数据）"""
        # 如果有正在运行的写入线程，先等待完成
        if self.db_writer and self.db_writer.isRunning():
            self._log("[信息] 等待数据写入完成后导出...")
            self.db_writer.stop()
            self.db_writer.wait()
        
        notices = self.db.get_notices(limit=None)
        if not notices:
            QMessageBox.warning(self, "警告", "没有可导出的数据")
            return
        
        file_path, _ = QFileDialog.getSaveFileName(
            self, "保存Excel", "公告数据.xlsx", "Excel文件 (*.xlsx)"
        )
        
        if file_path:
            exporter = ExcelExporter()
            success, error_msg = exporter.export(notices, file_path)
            if success:
                QMessageBox.information(self, "成功", f"已导出到: {file_path}")
                self._log(f"[信息] 导出Excel成功: {file_path}")
            else:
                error_detail = error_msg or "未知错误"
                QMessageBox.critical(self, "错误", f"导出失败:\n{error_detail}")
                self._log(f"[错误] 导出Excel失败: {error_detail}")
    
    def _export_log(self):
        """导出日志为txt文件"""
        log_content = self.log_text.toPlainText()
        if not log_content.strip():
            QMessageBox.information(self, "提示", "日志为空，无需导出")
            return


        default_name = f"日志_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        file_path, _ = QFileDialog.getSaveFileName(
            self, "导出日志", default_name, "文本文件 (*.txt)"
        )
        
        if file_path:
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(log_content)
                QMessageBox.information(self, "成功", f"已导出到: {file_path}")
                self._log(f"[信息] 导出日志成功: {file_path}")
            except Exception as e:
                QMessageBox.critical(self, "错误", f"导出失败: {str(e)}")
    
    def _run_custom_scripts(self):
        """选择并执行自定义Python脚本"""
        file_paths, _ = QFileDialog.getOpenFileNames(
            self, "选择Python脚本", "", "Python文件 (*.py)"
        )
        
        if not file_paths:
            return
        
        self._log(f"[信息] 准备执行 {len(file_paths)} 个脚本...")
        
        # 清空之前的数据
        self.db.clear_notices()
        self._load_table()
        
        # 启动脚本执行线程
        self.script_thread = ScriptWorker(file_paths)
        self.script_thread.progress.connect(self._on_script_progress)
        self.script_thread.log.connect(self._log)
        self.script_thread.result.connect(self._on_script_result)
        self.script_thread.finished.connect(self._on_script_finished)
        self.script_thread.start()
        
        self.run_script_btn.setEnabled(False)
        self.start_btn.setEnabled(False)
    
    def _on_script_progress(self, current, total):
        """脚本执行进度"""
        progress = int(current / total * 100) if total > 0 else 0
        self.progress_bar.setValue(progress)
        self.statusbar.showMessage(f"执行脚本... {current}/{total}")
    
    def _on_script_result(self, notice_data):
        """接收脚本执行结果"""
        try:
            notice = Notice(
                title=notice_data.get("title", ""),
                url=notice_data.get("url", ""),
                publish_date=notice_data.get("publish_date", ""),
                school=notice_data.get("school", ""),
                category=notice_data.get("category", ""),
                keywords=notice_data.get("keywords", ""),
                days_old=notice_data.get("days_old", 0)
            )
            self.db.add_notice(notice)
        except Exception as e:
            self._log(f"[错误] 保存脚本结果失败: {str(e)}")
    
    def _on_script_finished(self):
        """脚本执行完成"""
        self.run_script_btn.setEnabled(True)
        self.start_btn.setEnabled(True)
        self.progress_bar.setValue(100)
        self.statusbar.showMessage("脚本执行完成")
        self._log("[信息] 脚本执行完成")
        
        # 刷新表格
        self.current_page = 1
        self._load_table()
        
        # 显示结果
        count = self.db.get_notices_count()
        QMessageBox.information(self, "完成", f"脚本执行完成，共采集 {count} 条数据")
    
    def _clear_log(self):
        """清空日志"""
        self.log_text.clear()
    
    def _log(self, message):
        """添加日志"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.append(f"[{timestamp}] {message}")
    
    def closeEvent(self, event):
        """关闭事件"""
        self._stop_crawl()
        self.db.close()
        event.accept()