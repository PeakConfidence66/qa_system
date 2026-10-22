"""
配置文件
"""
import os
import sys

# 应用信息
APP_NAME = "CampusPulse 公告采集工具"
APP_VERSION = "1.0.0"

# 路径配置
# 兼容 PyInstaller 打包：exe 运行时 sys.frozen=True，_MEIPASS 是临时解压目录
# 数据库和日志应保存在 exe 所在目录（用户可持久化），而非临时目录
if getattr(sys, 'frozen', False):
    # 打包后：使用 exe 所在目录
    BASE_DIR = os.path.dirname(sys.executable)
else:
    # 开发时：使用脚本所在目录
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
LOGS_DIR = os.path.join(BASE_DIR, "logs")

# 确保目录存在
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

# 数据库配置
DATABASE_PATH = os.path.join(DATA_DIR, "campus_pulse.db")

# 学校类别选项
SCHOOL_CATEGORIES = [
    "985",
    "211", 
    "双一流",
    "双一流985",
    "双一流211",
    "普通"
]

# 采集时间范围选项
TIME_RANGES = {
    "1天内": 1,
    "3天内": 3,
    "7天内": 7,
    "30天内": 30,
    "365天内":365,
    "全部": 36500  # 约100年
}

# 默认请求头
DEFAULT_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "Cache-Control": "max-age=0",
    "Connection": "no-cache",
    "If-Modified-Since": None,
    "If-None-Match": None,
    "Sec-Fetch-Dest": "document",
    # "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36 Edg/148.0.0.0",
    "sec-ch-ua": "\"Chromium\";v=\"148\", \"Microsoft Edge\";v=\"148\", \"Not/A)Brand\";v=\"99\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\""
}

# 表格列定义
TABLE_COLUMNS = [
    "标题",
    "链接", 
    "发布日期",
    "学校",
    "类别",
    "命中关键词",
    "时效(天)"
]