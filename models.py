"""
数据模型
"""
import sqlite3
import json
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict, Any
from config import DATABASE_PATH


@dataclass
class School:
    """学校模型"""
    id: Optional[int] = None
    name: str = ""
    category: str = "普通"
    created_at: str = ""
    
    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


@dataclass
class College:
    """学院模型"""
    id: Optional[int] = None
    school_id: int = 0
    name: str = ""
    created_at: str = ""
    
    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


@dataclass
class Link:
    """链接模型"""
    id: Optional[int] = None
    college_id: int = 0
    url: str = ""
    link_type: str = "main"  # main 或 sub
    parse_rules: str = ""  # 解析规则 JSON
    created_at: str = ""
    
    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def get_rules(self) -> Dict[str, Any]:
        """获取解析规则"""
        try:
            return json.loads(self.parse_rules) if self.parse_rules else {}
        except:
            return {}
    
    def set_rules(self, rules: Dict[str, Any]):
        """设置解析规则"""
        self.parse_rules = json.dumps(rules, ensure_ascii=False)


@dataclass
class Notice:
    """公告模型"""
    id: Optional[int] = None
    title: str = ""
    url: str = ""
    publish_date: str = ""
    school: str = ""
    category: str = ""
    keywords: str = ""
    days_old: int = 0
    content: str = ""
    created_at: str = ""
    
    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class Database:
    """数据库操作类"""
    
    def __init__(self):
        self.conn = sqlite3.connect(DATABASE_PATH)
        self.conn.row_factory = sqlite3.Row
        self._init_tables()
    
    def _init_tables(self):
        """初始化数据表"""
        cursor = self.conn.cursor()
        
        # 学校表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS schools (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                category TEXT DEFAULT '普通',
                created_at TEXT
            )
        """)
        
        # 学院表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS colleges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                school_id INTEGER,
                name TEXT NOT NULL,
                created_at TEXT,
                FOREIGN KEY (school_id) REFERENCES schools(id)
            )
        """)
        
        # 链接表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                college_id INTEGER,
                url TEXT NOT NULL,
                link_type TEXT DEFAULT 'main',
                parse_rules TEXT,
                created_at TEXT,
                FOREIGN KEY (college_id) REFERENCES colleges(id)
            )
        """)
        
        # 公告表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS notices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                url TEXT,
                publish_date TEXT,
                school TEXT,
                category TEXT,
                keywords TEXT,
                days_old INTEGER,
                content TEXT,
                created_at TEXT
            )
        """)
        
        self.conn.commit()
    
    # ==================== 学校操作 ====================
    
    def add_school(self, school: School) -> int:
        """添加学校"""
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO schools (name, category, created_at) VALUES (?, ?, ?)",
            (school.name, school.category, school.created_at)
        )
        self.conn.commit()
        return cursor.lastrowid
    
    def get_schools(self) -> List[School]:
        """获取所有学校"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM schools ORDER BY name")
        rows = cursor.fetchall()
        return [School(**dict(row)) for row in rows]
    
    def get_school_by_id(self, school_id: int) -> Optional[School]:
        """根据ID获取学校"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM schools WHERE id = ?", (school_id,))
        row = cursor.fetchone()
        return School(**dict(row)) if row else None
    
    def get_school_by_name(self, name: str) -> Optional[School]:
        """根据名称获取学校"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM schools WHERE name = ?", (name,))
        row = cursor.fetchone()
        return School(**dict(row)) if row else None
    
    def delete_school(self, school_id: int):
        """删除学校（级联删除学院和链接）"""
        cursor = self.conn.cursor()
        # 先删除该学校所有学院下的链接
        colleges = self.get_colleges_by_school(school_id)
        for college in colleges:
            cursor.execute("DELETE FROM links WHERE college_id = ?", (college.id,))
        # 删除学院
        cursor.execute("DELETE FROM colleges WHERE school_id = ?", (school_id,))
        # 删除学校
        cursor.execute("DELETE FROM schools WHERE id = ?", (school_id,))
        self.conn.commit()
    
    # ==================== 学院操作 ====================
    
    def add_college(self, college: College) -> int:
        """添加学院"""
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO colleges (school_id, name, created_at) VALUES (?, ?, ?)",
            (college.school_id, college.name, college.created_at)
        )
        self.conn.commit()
        return cursor.lastrowid
    
    def get_colleges_by_school(self, school_id: int) -> List[College]:
        """获取学校的所有学院"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM colleges WHERE school_id = ? ORDER BY name", (school_id,))
        rows = cursor.fetchall()
        return [College(**dict(row)) for row in rows]
    
    def get_college_by_name(self, school_id: int, name: str) -> Optional[College]:
        """根据学校ID和学院名称获取学院"""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT * FROM colleges WHERE school_id = ? AND name = ?", 
            (school_id, name)
        )
        row = cursor.fetchone()
        return College(**dict(row)) if row else None
    
    def delete_college(self, college_id: int):
        """删除学院（级联删除链接）"""
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM links WHERE college_id = ?", (college_id,))
        cursor.execute("DELETE FROM colleges WHERE id = ?", (college_id,))
        self.conn.commit()
    
    # ==================== 链接操作 ====================
    
    def add_link(self, link: Link) -> int:
        """添加链接"""
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO links (college_id, url, link_type, parse_rules, created_at) VALUES (?, ?, ?, ?, ?)",
            (link.college_id, link.url, link.link_type, link.parse_rules, link.created_at)
        )
        self.conn.commit()
        return cursor.lastrowid
    
    def get_links_by_college(self, college_id: int) -> List[Link]:
        """获取学院的所有链接"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM links WHERE college_id = ?", (college_id,))
        rows = cursor.fetchall()
        return [Link(**dict(row)) for row in rows]
    
    def get_all_links(self) -> List[Dict]:
        """获取所有链接（带学校和学院信息）"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT l.*, c.name as college_name, c.school_id, s.name as school_name, s.category
            FROM links l
            JOIN colleges c ON l.college_id = c.id
            JOIN schools s ON c.school_id = s.id
        """)
        rows = cursor.fetchall()
        result = []
        for row in rows:
            d = dict(row)
            d['rules'] = json.loads(d['parse_rules']) if d['parse_rules'] else {}
            result.append(d)
        return result
    
    def delete_link(self, link_id: int):
        """删除链接"""
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM links WHERE id = ?", (link_id,))
        self.conn.commit()
    
    # ==================== 公告操作 ====================
    
    def add_notice(self, notice: Notice) -> int:
        """添加公告"""
        cursor = self.conn.cursor()
        cursor.execute(
            """INSERT INTO notices 
               (title, url, publish_date, school, category, keywords, days_old, content, created_at) 
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (notice.title, notice.url, notice.publish_date, notice.school,
             notice.category, notice.keywords, notice.days_old, notice.content, notice.created_at)
        )
        self.conn.commit()
        return cursor.lastrowid
    
    def get_notices(self, limit: int = None, offset: int = 0) -> List[Notice]:
        """获取公告列表（支持分页，limit=None表示获取全部）"""
        cursor = self.conn.cursor()
        if limit is None:
            cursor.execute("SELECT * FROM notices ORDER BY created_at DESC")
        else:
            cursor.execute(
                "SELECT * FROM notices ORDER BY created_at DESC LIMIT ? OFFSET ?",
                (limit, offset)
            )
        rows = cursor.fetchall()
        return [Notice(**dict(row)) for row in rows]
    
    def get_notices_count(self) -> int:
        """获取公告总数"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM notices")
        return cursor.fetchone()[0]
    
    def get_count_by_school_college(self) -> dict:
        """按学校和学院统计公告数量"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT school, category, COUNT(*) as cnt
            FROM notices
            GROUP BY school, category
        """)
        result = {}
        for row in cursor.fetchall():
            school, category, cnt = row
            if school not in result:
                result[school] = {}
            result[school][category] = cnt
        return result
    
    def get_count_by_url(self) -> dict:
        """按URL统计公告数量（精确到每个链接）"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT school, category, url, COUNT(*) as cnt
            FROM notices
            GROUP BY school, category, url
        """)
        result = {}
        for row in cursor.fetchall():
            school, category, url, cnt = row
            key = (school, category, url)
            result[key] = cnt
        return result
    
    def clear_notices(self):
        """清空公告"""
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM notices")
        self.conn.commit()
    
    def close(self):
        """关闭连接"""
        self.conn.close()