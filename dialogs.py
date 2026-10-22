"""
对话框模块
"""
from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, 
    QLineEdit, QComboBox, QPushButton, QTabWidget,
    QWidget, QFormLayout, QMessageBox,
    QGroupBox, QCheckBox
)
from PyQt6.QtCore import Qt
from models import School, College, Link, Database
from config import SCHOOL_CATEGORIES


class AddSchoolDialog(QDialog):
    """添加学校对话框（简化版，只输入学校名称）"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("添加学校")
        self.setFixedSize(400, 150)
        self.db = Database()
        self._setup_ui()
    
    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(15)
        layout.setContentsMargins(20, 20, 20, 20)
        
        # 学校名称
        name_layout = QHBoxLayout()
        name_label = QLabel("学校名称:")
        self.name_input = QLineEdit()
        self.name_input.setPlaceholderText("请输入学校名称")
        name_layout.addWidget(name_label)
        name_layout.addWidget(self.name_input)
        layout.addLayout(name_layout)
        
        layout.addStretch()
        
        # 按钮
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        
        self.ok_btn = QPushButton("确定")
        self.ok_btn.setStyleSheet("""
            QPushButton {
                background-color: #1890ff;
                color: white;
                border: none;
                padding: 8px 20px;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #40a9ff;
            }
        """)
        self.ok_btn.clicked.connect(self._on_ok)
        
        self.cancel_btn = QPushButton("取消")
        self.cancel_btn.setStyleSheet("""
            QPushButton {
                background-color: #f0f0f0;
                border: 1px solid #d9d9d9;
                padding: 8px 20px;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #e6e6e6;
            }
        """)
        self.cancel_btn.clicked.connect(self.reject)
        
        btn_layout.addWidget(self.ok_btn)
        btn_layout.addWidget(self.cancel_btn)
        layout.addLayout(btn_layout)
    
    def _on_ok(self):
        name = self.name_input.text().strip()
        if not name:
            QMessageBox.warning(self, "警告", "请输入学校名称")
            return
        
        # 检查学校是否已存在
        existing = self.db.get_school_by_name(name)
        if existing:
            QMessageBox.warning(self, "提示", f"学校「{name}」已存在，请勿重复添加")
            return
        
        self.accept()
    
    def get_school_name(self):
        """获取学校名称"""
        return self.name_input.text().strip()


class DataSourceDialog(QDialog):
    """数据源配置对话框（添加学校后自动弹出）"""
    
    def __init__(self, school_name, parent=None):
        super().__init__(parent)
        self.setWindowTitle("数据源")
        self.setMinimumSize(600, 450)
        self.db = Database()
        self.school_name = school_name
        self.school_id = None
        self._setup_ui()
    
    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(15, 15, 15, 15)
        
        # 标签页
        self.tab_widget = QTabWidget()
        
        # 基本信息标签页
        self.basic_tab = QWidget()
        self._setup_basic_tab()
        self.tab_widget.addTab(self.basic_tab, "基本信息")
        
        # 解析规则标签页
        self.rule_tab = QWidget()
        self._setup_rule_tab()
        self.tab_widget.addTab(self.rule_tab, "解析规则")
        
        layout.addWidget(self.tab_widget)
        
        # 按钮
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        
        self.ok_btn = QPushButton("OK")
        self.ok_btn.setStyleSheet("""
            QPushButton {
                background-color: #1890ff;
                color: white;
                border: none;
                padding: 8px 25px;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #40a9ff;
            }
        """)
        self.ok_btn.clicked.connect(self._on_ok)
        
        self.cancel_btn = QPushButton("Cancel")
        self.cancel_btn.setStyleSheet("""
            QPushButton {
                background-color: #f0f0f0;
                border: 1px solid #d9d9d9;
                padding: 8px 25px;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #e6e6e6;
            }
        """)
        self.cancel_btn.clicked.connect(self.reject)
        
        btn_layout.addWidget(self.ok_btn)
        btn_layout.addWidget(self.cancel_btn)
        layout.addLayout(btn_layout)
    
    def _setup_basic_tab(self):
        """设置基本信息标签页"""
        layout = QFormLayout(self.basic_tab)
        layout.setSpacing(12)
        layout.setContentsMargins(20, 20, 20, 20)
        
        # 学校名称（只读）
        self.school_name_input = QLineEdit(self.school_name)
        self.school_name_input.setReadOnly(True)
        layout.addRow("学校名称*", self.school_name_input)
        
        # 学校类别
        self.category_combo = QComboBox()
        self.category_combo.addItems(SCHOOL_CATEGORIES)
        self.category_combo.setCurrentText("普通")
        layout.addRow("学校类别", self.category_combo)
        
        # 学院名称
        self.college_input = QLineEdit()
        self.college_input.setPlaceholderText("可选，默认：主站")
        layout.addRow("学院名称", self.college_input)
        
        # 学院链接
        self.college_url_input = QLineEdit()
        self.college_url_input.setPlaceholderText("可选，学院站点首页")
        layout.addRow("学院链接", self.college_url_input)
        
        # 主链接（列表）
        self.main_url_input = QLineEdit()
        self.main_url_input.setPlaceholderText("")
        layout.addRow("主链接（列表）*", self.main_url_input)
        
        # 副链接（列表）
        self.sub_url_input = QLineEdit()
        self.sub_url_input.setPlaceholderText("可选，第二个列表页；与主链接去重后合并")
        layout.addRow("副链接（列表）", self.sub_url_input)
    
    def _setup_rule_tab(self):
        """设置解析规则标签页"""
        layout = QVBoxLayout(self.rule_tab)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(15)
        
        # 标题规则
        title_layout = QHBoxLayout()
        title_label = QLabel("标题规则")
        self.title_type_combo = QComboBox()
        self.title_type_combo.addItems(["xpath", "css", "regex"])
        self.title_type_combo.setCurrentText("xpath")
        self.title_input = QLineEdit()
        title_layout.addWidget(title_label)
        title_layout.addWidget(self.title_type_combo)
        title_layout.addWidget(self.title_input, 1)
        layout.addLayout(title_layout)
        
        # 日期规则
        date_layout = QHBoxLayout()
        date_label = QLabel("日期规则")
        self.date_type_combo = QComboBox()
        self.date_type_combo.addItems(["xpath", "css", "regex"])
        self.date_type_combo.setCurrentText("xpath")
        self.date_input = QLineEdit()
        date_layout.addWidget(date_label)
        date_layout.addWidget(self.date_type_combo)
        date_layout.addWidget(self.date_input, 1)
        layout.addLayout(date_layout)
        
        # 链接规则
        link_layout = QHBoxLayout()
        link_label = QLabel("链接规则")
        self.link_type_combo = QComboBox()
        self.link_type_combo.addItems(["xpath", "css", "regex"])
        self.link_type_combo.setCurrentText("css")
        self.link_input = QLineEdit()
        link_layout.addWidget(link_label)
        link_layout.addWidget(self.link_type_combo)
        link_layout.addWidget(self.link_input, 1)
        layout.addLayout(link_layout)
        
        # 翻页按钮
        page_layout = QHBoxLayout()
        page_label = QLabel("翻页按钮")
        self.page_type_combo = QComboBox()
        self.page_type_combo.addItems(["xpath", "css", "regex"])
        self.page_type_combo.setCurrentText("css")
        self.page_input = QLineEdit()
        page_layout.addWidget(page_label)
        page_layout.addWidget(self.page_type_combo)
        page_layout.addWidget(self.page_input, 1)
        layout.addLayout(page_layout)
        
        # 自动识别复选框
        self.auto_detect_cb = QCheckBox("自动识别（未配置规则时尝试）")
        self.auto_detect_cb.setChecked(True)
        layout.addWidget(self.auto_detect_cb)
        
        layout.addStretch()
    
    def _on_ok(self):
        """确定按钮"""
        school_name = self.school_name_input.text().strip()
        main_url = self.main_url_input.text().strip()
        
        if not main_url:
            QMessageBox.warning(self, "警告", "请输入主链接（列表）")
            return
        
        try:
            # 创建学校
            school = School(
                name=school_name,
                category=self.category_combo.currentText()
            )
            self.school_id = self.db.add_school(school)
            
            # 创建学院
            college_name = self.college_input.text().strip() or "主站"
            college = College(
                school_id=self.school_id,
                name=college_name
            )
            college_id = self.db.add_college(college)
            
            # 创建主链接
            main_link = Link(
                college_id=college_id,
                url=main_url,
                link_type='main'
            )
            main_link.set_rules({
                "title_rule": self.title_input.text().strip(),
                "title_type": self.title_type_combo.currentText(),
                "link_rule": self.link_input.text().strip(),
                "link_type": self.link_type_combo.currentText(),
                "date_rule": self.date_input.text().strip(),
                "date_type": self.date_type_combo.currentText(),
                "page_rule": self.page_input.text().strip(),
                "page_type": self.page_type_combo.currentText(),
                "auto_detect": self.auto_detect_cb.isChecked()
            })
            self.db.add_link(main_link)
            
            # 创建副链接（如果有）
            sub_url = self.sub_url_input.text().strip()
            if sub_url:
                sub_link = Link(
                    college_id=college_id,
                    url=sub_url,
                    link_type='sub'
                )
                sub_link.set_rules({
                    "title_rule": self.title_input.text().strip(),
                    "title_type": self.title_type_combo.currentText(),
                    "link_rule": self.link_input.text().strip(),
                    "link_type": self.link_type_combo.currentText(),
                    "date_rule": self.date_input.text().strip(),
                    "date_type": self.date_type_combo.currentText(),
                    "page_rule": self.page_input.text().strip(),
                    "page_type": self.page_type_combo.currentText(),
                    "auto_detect": self.auto_detect_cb.isChecked()
                })
                self.db.add_link(sub_link)
            
            self.accept()
        except Exception as e:
            QMessageBox.critical(self, "错误", f"保存失败: {str(e)}")


class EditLinkDialog(QDialog):
    """编辑链接对话框（编辑解析规则）"""
    
    def __init__(self, link_id=None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("编辑链接规则")
        self.setMinimumSize(500, 400)
        self.db = Database()
        self.link_id = link_id
        self.link = None
        
        if link_id:
            # 从数据库查找链接
            all_links = self.db.get_all_links()
            for l in all_links:
                if l['id'] == link_id:
                    self.link = l
                    break
        
        self._setup_ui()
        self._load_data()
    
    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(15, 15, 15, 15)
        
        # 标签页
        self.tab_widget = QTabWidget()
        
        # 基本信息标签页
        self.basic_tab = QWidget()
        self._setup_basic_tab()
        self.tab_widget.addTab(self.basic_tab, "基本信息")
        
        # 解析规则标签页
        self.rule_tab = QWidget()
        self._setup_rule_tab()
        self.tab_widget.addTab(self.rule_tab, "解析规则")
        
        layout.addWidget(self.tab_widget)
        
        # 按钮
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        
        self.ok_btn = QPushButton("确定")
        self.ok_btn.setStyleSheet("""
            QPushButton {
                background-color: #1890ff;
                color: white;
                border: none;
                padding: 8px 20px;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #40a9ff;
            }
        """)
        self.ok_btn.clicked.connect(self._on_ok)
        
        self.cancel_btn = QPushButton("取消")
        self.cancel_btn.setStyleSheet("""
            QPushButton {
                background-color: #f0f0f0;
                border: 1px solid #d9d9d9;
                padding: 8px 20px;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #e6e6e6;
            }
        """)
        self.cancel_btn.clicked.connect(self.reject)
        
        btn_layout.addWidget(self.ok_btn)
        btn_layout.addWidget(self.cancel_btn)
        layout.addLayout(btn_layout)
    
    def _setup_basic_tab(self):
        """设置基本信息标签页"""
        layout = QFormLayout(self.basic_tab)
        layout.setSpacing(15)
        layout.setContentsMargins(20, 20, 20, 20)
        
        self.url_input = QLineEdit()
        self.url_input.setPlaceholderText("链接URL")
        layout.addRow("链接URL*", self.url_input)
        
        self.type_combo = QComboBox()
        self.type_combo.addItems(["main", "sub"])
        layout.addRow("链接类型", self.type_combo)
    
    def _setup_rule_tab(self):
        """设置解析规则标签页 - 支持 xpath/css 选择"""
        layout = QVBoxLayout(self.rule_tab)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(15)
        
        # 标题规则
        title_layout = QHBoxLayout()
        title_label = QLabel("标题规则")
        self.title_type_combo = QComboBox()
        self.title_type_combo.addItems(["xpath", "css", "regex"])
        self.title_type_combo.setCurrentText("css")
        self.title_input = QLineEdit()
        title_layout.addWidget(title_label)
        title_layout.addWidget(self.title_type_combo)
        title_layout.addWidget(self.title_input, 1)
        layout.addLayout(title_layout)
        
        # 日期规则
        date_layout = QHBoxLayout()
        date_label = QLabel("日期规则")
        self.date_type_combo = QComboBox()
        self.date_type_combo.addItems(["xpath", "css", "regex"])
        self.date_type_combo.setCurrentText("css")
        self.date_input = QLineEdit()
        date_layout.addWidget(date_label)
        date_layout.addWidget(self.date_type_combo)
        date_layout.addWidget(self.date_input, 1)
        layout.addLayout(date_layout)
        
        # 链接规则
        link_layout = QHBoxLayout()
        link_label = QLabel("链接规则")
        self.link_type_combo = QComboBox()
        self.link_type_combo.addItems(["xpath", "css", "regex"])
        self.link_type_combo.setCurrentText("css")
        self.link_input = QLineEdit()
        link_layout.addWidget(link_label)
        link_layout.addWidget(self.link_type_combo)
        link_layout.addWidget(self.link_input, 1)
        layout.addLayout(link_layout)
        
        # 翻页按钮
        page_layout = QHBoxLayout()
        page_label = QLabel("翻页按钮")
        self.page_type_combo = QComboBox()
        self.page_type_combo.addItems(["xpath", "css", "regex"])
        self.page_type_combo.setCurrentText("css")
        self.page_input = QLineEdit()
        page_layout.addWidget(page_label)
        page_layout.addWidget(self.page_type_combo)
        page_layout.addWidget(self.page_input, 1)
        layout.addLayout(page_layout)
        
        # 自动识别复选框
        self.auto_detect_cb = QCheckBox("自动识别（未配置规则时尝试）")
        self.auto_detect_cb.setChecked(True)
        layout.addWidget(self.auto_detect_cb)
        
        layout.addStretch()
    
    def _load_data(self):
        """加载数据"""
        if self.link:
            self.url_input.setText(self.link.get('url', ''))
            
            link_type = self.link.get('link_type', 'main')
            idx = self.type_combo.findText(link_type)
            if idx >= 0:
                self.type_combo.setCurrentIndex(idx)
            
            rules = self.link.get('rules', {})
            self.title_input.setText(rules.get("title_rule", ""))
            title_type = rules.get("title_type", "css")
            idx = self.title_type_combo.findText(title_type)
            if idx >= 0:
                self.title_type_combo.setCurrentIndex(idx)
            
            self.date_input.setText(rules.get("date_rule", ""))
            date_type = rules.get("date_type", "css")
            idx = self.date_type_combo.findText(date_type)
            if idx >= 0:
                self.date_type_combo.setCurrentIndex(idx)
            
            self.link_input.setText(rules.get("link_rule", ""))
            link_type_rule = rules.get("link_type", "css")
            idx = self.link_type_combo.findText(link_type_rule)
            if idx >= 0:
                self.link_type_combo.setCurrentIndex(idx)
            
            self.page_input.setText(rules.get("page_rule", ""))
            page_type = rules.get("page_type", "css")
            idx = self.page_type_combo.findText(page_type)
            if idx >= 0:
                self.page_type_combo.setCurrentIndex(idx)
            
            self.auto_detect_cb.setChecked(rules.get("auto_detect", True))
    
    def _on_ok(self):
        """确定按钮"""
        url = self.url_input.text().strip()
        if not url:
            QMessageBox.warning(self, "警告", "请输入链接URL")
            return
        
        rules = {
            "title_rule": self.title_input.text().strip(),
            "title_type": self.title_type_combo.currentText(),
            "link_rule": self.link_input.text().strip(),
            "link_type": self.link_type_combo.currentText(),
            "date_rule": self.date_input.text().strip(),
            "date_type": self.date_type_combo.currentText(),
            "page_rule": self.page_input.text().strip(),
            "page_type": self.page_type_combo.currentText(),
            "auto_detect": self.auto_detect_cb.isChecked()
        }
        
        try:
            if self.link_id:
                # 更新链接
                import json
                cursor = self.db.conn.cursor()
                cursor.execute(
                    "UPDATE links SET url=?, link_type=?, parse_rules=? WHERE id=?",
                    (url, self.type_combo.currentText(), json.dumps(rules, ensure_ascii=False), self.link_id)
                )
                self.db.conn.commit()
            
            self.accept()
        except Exception as e:
            QMessageBox.critical(self, "错误", f"保存失败: {str(e)}")


class AddCollegeDialog(QDialog):
    """添加学院对话框"""
    
    def __init__(self, school_id, parent=None):
        super().__init__(parent)
        self.school_id = school_id
        self.db = Database()
        self.setWindowTitle("添加学院")
        self.setFixedSize(400, 150)
        self._setup_ui()
    
    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(15)
        layout.setContentsMargins(20, 20, 20, 20)
        
        name_layout = QHBoxLayout()
        name_label = QLabel("学院名称:")
        self.name_input = QLineEdit()
        self.name_input.setPlaceholderText("请输入学院名称")
        name_layout.addWidget(name_label)
        name_layout.addWidget(self.name_input)
        layout.addLayout(name_layout)
        
        layout.addStretch()
        
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        
        self.ok_btn = QPushButton("确定")
        self.ok_btn.setStyleSheet("""
            QPushButton {
                background-color: #1890ff;
                color: white;
                border: none;
                padding: 8px 20px;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #40a9ff;
            }
        """)
        self.ok_btn.clicked.connect(self._on_ok)
        
        self.cancel_btn = QPushButton("取消")
        self.cancel_btn.setStyleSheet("""
            QPushButton {
                background-color: #f0f0f0;
                border: 1px solid #d9d9d9;
                padding: 8px 20px;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #e6e6e6;
            }
        """)
        self.cancel_btn.clicked.connect(self.reject)
        
        btn_layout.addWidget(self.ok_btn)
        btn_layout.addWidget(self.cancel_btn)
        layout.addLayout(btn_layout)
    
    def _on_ok(self):
        """确定按钮：保存学院到数据库"""
        name = self.name_input.text().strip()
        if not name:
            QMessageBox.warning(self, "警告", "请输入学院名称")
            return
        
        try:
            college = College(
                school_id=self.school_id,
                name=name
            )
            self.db.add_college(college)
            self.accept()
        except Exception as e:
            QMessageBox.critical(self, "错误", f"保存失败: {str(e)}")
    
    def get_college_name(self):
        return self.name_input.text().strip()