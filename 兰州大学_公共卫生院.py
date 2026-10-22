# lzu_sph_spider.py
import json
import re
import time
import requests

url = "https://sph.lzu.edu.cn/sph/lm/103830.js"
BASE_HOST = "https://sph.lzu.edu.cn"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://sph.lzu.edu.cn/",
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.9",
}


def fetch_articles():
    """请求接口并解析为文章列表（不需要翻页）"""
    params = {
        "timestamp": int(time.time() * 1000),
        "_": int(time.time() * 1000),
    }
    resp = requests.get(url, headers=headers, params=params, timeout=15)
    resp.encoding = "utf-8"

    try:
        data = resp.json()
    except json.JSONDecodeError:
        # 兜底：处理返回 JS 字面量（key 未加引号）的情况
        text = resp.text.strip()
        if text.startswith("var"):
            text = text.split("=", 1)[1].rstrip(";")
        text = re.sub(r"([{,]\s*)([A-Za-z_][A-Za-z0-9_]*)(\s*:)", r'\1"\2"\3', text)
        data = json.loads(text)

    if isinstance(data, dict):
        data = data.get("data") or data.get("list") or data.get("rows") or []
    return data or []


def scrape_announcements():
    """抓取全部公告并逐条调用 emit_result 提交"""
    items = fetch_articles()
    count = 0

    for art in items:
        # 核心字段
        title = (art.get("articleTitle") or art.get("textTitle") or "").strip()
        rel_url = art.get("articleUrl") or art.get("aurl") or ""
        publish_date = (art.get("publishDate") or art.get("createTime") or "").strip()

        if not title or not rel_url:
            continue

        # 拼接完整 URL
        if rel_url.startswith("http"):
            full_url = rel_url
        elif rel_url.startswith("/"):
            full_url = BASE_HOST + rel_url
        else:
            full_url = BASE_HOST + "/" + rel_url

        # 使用 emit_result 提交单条数据
        emit_result({
            "title": title,            # 标题（必填）
            "url": full_url,           # 链接（必填）
            "publish_date": publish_date,  # 发布日期
            "school": "兰州大学",       # 学校名称
            "category": "公共卫生学院",  # 类别
            "keywords": "公共卫生学院",  # 关键词
            "days_old": 0,             # 时效（天），可在外部统一计算
        })
        count += 1

    print(f"采集任务完成，共采集 {count} 条")
    return count


# 入口
if __name__ == "__main__":
    scrape_announcements()
