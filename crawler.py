"""
爬虫模块 - 支持 xpath/css/regex + POST+JSON 接口
优化版：模块化重构自动识别引擎、高容错分裂日期重组与多线程并发
日志增强版：提供更清晰的状态码、翻页进度及采集过滤日志
"""
import re
import json
import threading
import queue
from datetime import datetime
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from curl_cffi import requests as curl_requests
from bs4 import BeautifulSoup
from lxml import etree
from dateutil import parser
from PyQt6.QtCore import QThread, pyqtSignal

from models import Notice, Database
from config import DEFAULT_HEADERS


class PreloadWorker(QThread):
    """预加载线程 - 在后台获取链接和清空数据，避免UI卡顿"""

    progress = pyqtSignal(str)  # 进度消息
    finished = pyqtSignal(list, list)  # 返回(链接列表, 选中的item数据列表)

    def __init__(self, db, range_mode="all", selected_items=None):
        """
        Args:
            db: 数据库实例（主线程的）
            range_mode: 采集范围 ("all", "selected", "school")
            selected_items: 选中的item数据列表 [{"type": "school/college/link", "id": xxx}, ...]
        """
        super().__init__()
        self._db = db  # 保存数据库路径信息
        self.db = None  # 会在run()中创建新连接
        self._running = True
        self.range_mode = range_mode
        self.selected_items = selected_items or []

    def run(self):
        """后台预加载数据"""
        try:
            # 在当前线程创建数据库连接
            self.db = Database()

            self.progress.emit("[准备] 获取数据源列表...")
            sources = self._get_selected_sources()

            self.progress.emit(f"[准备] 获取到 {len(sources)} 个链接，正在清空旧数据...")
            self.db.clear_notices()
            self.db.close()  # 关闭连接

            self.progress.emit("[准备] 数据准备完成！")
            self.finished.emit(sources, self.selected_items)

        except Exception as e:
            import traceback
            error_msg = traceback.format_exc()
            self.progress.emit(f"[错误] 准备数据失败: {str(e)}")
            if self.db:
                self.db.close()
            self.finished.emit([], [])

    def _get_selected_sources(self):
        """获取选中的链接列表"""
        all_links = self.db.get_all_links()

        if self.range_mode == "all" or not self.selected_items:
            return all_links

        result_links = []
        for item_data in self.selected_items:
            item_type = item_data.get("type")
            item_id = item_data.get("id")

            if item_type == "link":
                link = next((l for l in all_links if l['id'] == item_id), None)
                if link and link not in result_links:
                    result_links.append(link)
            elif item_type == "college":
                college_links = [l for l in all_links if l.get('college_id') == item_id]
                for link in college_links:
                    if link not in result_links:
                        result_links.append(link)
            elif item_type == "school":
                school_links = [l for l in all_links if l.get('school_id') == item_id]
                for link in school_links:
                    if link not in result_links:
                        result_links.append(link)

        return result_links

    def stop(self):
        self._running = False


class DBWriterWorker(QThread):
    """专门的数据库写入线程 - 完全在后台处理，不阻塞主线程"""

    log = pyqtSignal(str)  # 日志信号
    finished = pyqtSignal()  # 完成信号

    def __init__(self):
        super().__init__()
        self.db = None
        self._queue = queue.Queue()
        self._running = True
        self._batch_size = 100  # 每批写入100条
        self._flush_interval = 3.0  # 每3秒强制刷新一次

    def add_notices(self, notices):
        """添加公告到队列"""
        for notice_data in notices:
            self._queue.put(notice_data)

    def run(self):
        """后台写入数据库"""
        try:
            self.db = Database()  # 在当前线程创建连接
            batch = []
            import time
            last_flush = time.time()

            while self._running:
                try:
                    # 带超时的获取，避免无限等待
                    item = self._queue.get(timeout=0.5)
                    batch.append(item)

                    # 批量写入或超时刷新
                    current_time = time.time()
                    if len(batch) >= self._batch_size or (current_time - last_flush) >= self._flush_interval:
                        self._write_batch(batch)
                        batch.clear()
                        last_flush = current_time

                except queue.Empty:
                    # 超时，检查是否需要刷新
                    if batch and (time.time() - last_flush) >= self._flush_interval:
                        self._write_batch(batch)
                        batch.clear()
                        last_flush = time.time()
                    continue

            # 写入剩余数据
            if batch:
                self._write_batch(batch)

            self.db.close()
            self.finished.emit()

        except Exception as e:
            import traceback
            self.log.emit(f"[错误] 数据库写入线程异常: {str(e)}")
            if self.db:
                self.db.close()
            self.finished.emit()

    def _write_batch(self, batch):
        """批量写入"""
        try:
            count = 0
            for notice_data in batch:
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
                count += 1
            self.log.emit(f"[写入] 批量保存 {count} 条公告")
        except Exception as e:
            self.log.emit(f"[错误] 批量写入失败: {str(e)}")

    def stop(self):
        """停止写入"""
        self._running = False


class CrawlerWorker(QThread):
    """爬虫工作线程"""

    progress = pyqtSignal(int, int)
    log = pyqtSignal(str)
    result_summary = pyqtSignal(dict)
    notice_found = pyqtSignal(dict)
    finished = pyqtSignal()

    def __init__(self, links, days=7, include_keywords="", exclude_keywords="", max_workers=3):
        super().__init__()
        self.links = links

        try:
            self.days = int(days)
        except (ValueError, TypeError):
            self.days = 7

        self.include_keywords = [k.strip() for k in re.split(r'[,，]', include_keywords) if k.strip()]
        self.exclude_keywords = [k.strip() for k in re.split(r'[,，]', exclude_keywords) if k.strip()]
        self.max_workers = max_workers
        self._running = True
        self.total_collected = 0
        self.total_after_filter = 0
        self.seen_urls = set()
        self._lock = threading.Lock()
        self._completed_count = 0

        # 🌟 初始化 use_scrapling 变量，防止 AttributeError
        self.use_scrapling = False

        # 统计信息
        self.link_stats = []  # 每个链接的采集结果 [{"school", "college", "url", "count", "success", "error"}]

    def _recalc_current_dates(self, current_list):
        """辅助函数：为手工规则命中的列表元素统一计算 days_old 和标准格式化时间"""
        for item in current_list:
            formatted_date, days_old, success = self._parse_date(item['date'])
            item['date'] = formatted_date if success else item['date']
            item['days_old'] = days_old

    def run(self):
        total = len(self.links)
        self.total_collected = 0
        self.total_after_filter = 0
        self.seen_urls = set()
        self._completed_count = 0
        self.link_stats = []  # 重置统计

        # # 🌟 重置 scrapling 状态
        # self.use_scrapling = False

        self.log.emit(f"[配置] 包含关键词: {self.include_keywords if self.include_keywords else '(无)'}")
        self.log.emit(f"[配置] 排除关键词: {self.exclude_keywords if self.exclude_keywords else '(无)'}")
        self.log.emit(f"[配置] 线程并发数: {self.max_workers} | 目标链接总数: {total}")

        if self.max_workers <= 1 or total <= 1:
            # 单线程模式
            for i, link_info in enumerate(self.links):
                if not self._running:
                    break
                self.progress.emit(i + 1, total)
                self._process_single_link(link_info)
        else:
            # 多线程模式
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_link = {
                    executor.submit(self._process_single_link, link_info): link_info
                    for link_info in self.links
                }

                for future in as_completed(future_to_link):
                    if not self._running:
                        break

                    link_info = future_to_link[future]
                    try:
                        future.result()
                    except Exception as e:
                        url = link_info.get('url', '')
                        self.log.emit(f"[线程 | 异常] 链路中断 -> {url} : {str(e)}")

                    with self._lock:
                        self._completed_count += 1
                        self.progress.emit(self._completed_count, total)

        summary = {
            "total_collected": self.total_collected,
            "total_after_filter": self.total_after_filter,
            "days": self.days,
            "include_keywords": ", ".join(self.include_keywords) if self.include_keywords else "(无)",
            "exclude_keywords": ", ".join(self.exclude_keywords) if self.exclude_keywords else "(无)",
            "link_stats": self.link_stats.copy(),  # 链接采集统计
        }
        self.result_summary.emit(summary)
        self.finished.emit()

    def _process_single_link(self, link_info):
        """处理单个链接（用于线程池）"""
        if not self._running:
            return

        url = link_info.get('url', '')
        school_name = link_info.get('school_name', '')
        college_name = link_info.get('college_name', '')
        rules = link_info.get('rules', {})

        # 初始化统计记录
        stat = {
            "school": school_name,
            "college": college_name,
            "url": url,
            "count": 0,
            "collected_count": 0,  # 解析出的原始数量
            "success": False,
            "has_warning": False,  # 是否有警告（如404状态码）
            "warnings": [],  # 警告信息列表
            "error": None
        }

        if not url:
            return

        self.log.emit(f"[开始 | 采集] 学校: {school_name} | 部门: {college_name}")

        try:
            result = self._crawl_with_pagination(url, school_name, college_name, rules)
            # result 可能返回 (count, warnings, collected_count)
            if isinstance(result, tuple):
                stat["collected_count"] = result[0] if len(result) > 0 else 0
                stat["warnings"] = result[1] if len(result) > 1 else []
                stat["count"] = result[2] if len(result) > 2 else stat["collected_count"]
            else:
                stat["count"] = result
                stat["collected_count"] = result

            stat["has_warning"] = len(stat["warnings"]) > 0
            stat["success"] = True

            if stat["warnings"]:
                warn_msg = "; ".join(stat["warnings"])
                self.log.emit(
                    f"[结束 | 成功] 目标: {school_name}-{college_name} | 入库 {stat['count']} 条(共解析 {stat['collected_count']} 条) | ⚠ {warn_msg}")
            else:
                self.log.emit(
                    f"[结束 | 成功] 目标: {school_name}-{college_name} | 入库 {stat['count']} 条(共解析 {stat['collected_count']} 条)")
        except Exception as e:
            stat["success"] = False
            stat["error"] = str(e)
            self.log.emit(f"[结束 | 失败] 目标: {school_name}-{college_name} | 原因: {str(e)}")

        # 记录统计
        with self._lock:
            self.link_stats.append(stat)

    # ====================== 统一请求（支持 412/202 状态码无头浏览器自愈） ======================
    def _make_request(self, url, rules, page=1):
        """统一请求方法，支持 GET / POST + 增强编码处理 + curl_cffi 兜底"""
        method = rules.get("request_method", "GET").upper()
        headers = DEFAULT_HEADERS.copy()

        resp = None
        # 第一次尝试：普通 requests
        try:
            if method == "POST":
                post_data = rules.get("request_data", {}).copy()
                for key in ["page", "pageNum", "current", "pn", "pageIndex", "p"]:
                    if key in post_data:
                        post_data[key] = page

                if rules.get("json_body", False):
                    resp = requests.post(url, json=post_data, headers=headers, timeout=30)
                else:
                    resp = requests.post(url, data=post_data, headers=headers, timeout=30, verify=False)
            else:
                cookies = {
                    "client_id": "cid_jBEo2HVPZfDwnEKkNv4GMkHD2ZP9lCdZ",
                    "JSESSIONID": "049878F9036307D11F43390EEFD7660A"
                }
                resp = requests.get(url, headers=headers, cookies=cookies, timeout=30, verify=False)

            if resp is not None:
                self.log.emit(f"[请求 | 响应] 方法: {method} | 状态码: {resp.status_code} | URL: {url} (requests)")
                if 200 <= resp.status_code < 300:
                    return self._process_response(resp)

        except Exception as e:
            self.log.emit(f"[请求 | 重试] {method} {url} requests失败，准备切换底层 -> 原因: {str(e)}")

        # 第二次尝试：使用 curl_cffi
        try:
            if method == "POST":
                post_data = rules.get("request_data", {}).copy()
                for key in ["page", "pageNum", "current", "pn", "pageIndex", "p"]:
                    if key in post_data:
                        post_data[key] = page

                if rules.get("json_body", False):
                    resp = curl_requests.post(url, json=post_data, headers=headers, timeout=30)
                else:
                    resp = curl_requests.post(url, data=post_data, headers=headers, timeout=30, verify=False,
                                              impersonate="chrome110")
            else:
                resp = curl_requests.get(url, headers=headers, timeout=30, verify=False)

            if resp is not None:
                self.log.emit(f"[请求 | 响应] 方法: {method} | 状态码: {resp.status_code} | URL: {url} (curl_cffi)")
                if 200 <= resp.status_code < 300:
                    return self._process_response(resp)
        except Exception as e:
            self.log.emit(f"[请求 | 失败] 双层框架重试均告失败 -> URL: {url} | 原因: {str(e)}")
            return None

        return None

    def _process_response(self, resp):
        """统一处理响应编码（升级版：含深度编码探测日志与 Meta-Refresh 动态跳转）"""
        if resp is None:
            return None

        # ==================== 编码处理 ====================
        encoding = None
        # 1. 优先采用apparent_encoding
        if hasattr(resp, 'apparent_encoding') and resp.apparent_encoding:
            encoding = resp.apparent_encoding

        # 2. 如果不理想，尝试从 headers 中获取 charset
        if not encoding or encoding.lower() in ['iso-8859-1', 'ascii']:
            content_type = resp.headers.get('Content-Type', '')
            if 'charset=' in content_type:
                try:
                    encoding = content_type.split('charset=')[-1].strip().lower()
                except:
                    pass

        # 3. 兜底检测中文字符集
        if not encoding or encoding.lower() in ['iso-8859-1', 'ascii', '']:
            content = resp.content
            for enc in ['utf-8', 'gbk', 'gb2312', 'gb18030']:
                try:
                    text = content.decode(enc, errors='replace')
                    if len(text) > 100 and any('\u4e00' <= c <= '\u9fff' for c in text[:500]):
                        encoding = enc
                        break
                except:
                    continue

        resp.encoding = encoding or 'utf-8'

        # 🌟 打印编码诊断日志，帮我们一眼看清是否存在编码偏差
        self.log.emit(
            f"[请求 | 编码] 响应状态码: {resp.status_code} | 最终决定的解码集: {resp.encoding} | 备用apparent_encoding: {getattr(resp, 'apparent_encoding', '')}")

        # =========================================================================
        # 🌟 智能解析并执行 HTML 级 Meta-Refresh 重定向
        # =========================================================================
        html_content = resp.text
        meta_match = re.search(
            r'<meta[^>]*?http-equiv=["\']?refresh["\']?[^>]*?content=["\']?\d+\s*;\s*url=(.*?)["\']?',
            html_content,
            re.IGNORECASE
        )
        if meta_match:
            redirect_url = meta_match.group(1).strip()
            redirect_url = redirect_url.strip('\'"')
            if redirect_url:
                target_url = urljoin(resp.url, redirect_url)
                self.log.emit(f"[请求 | Meta跳转] 侦测到网页级 Meta-Refresh 动态重定向 -> {target_url}")
                try:
                    headers = DEFAULT_HEADERS.copy()
                    new_resp = curl_requests.get(target_url, headers=headers, timeout=30, verify=False,
                                                 impersonate="chrome110")
                    if new_resp and 200 <= new_resp.status_code < 300:
                        return self._process_response(new_resp)
                except Exception as e:
                    self.log.emit(f"[请求 | Meta跳转失败] 递归跳转请求失败: {e}")

        return resp

    # ====================== 翻页主逻辑 ======================
    def _crawl_with_pagination(self, start_url, school_name, college_name, rules):
        """带翻页采集 - 升级：引入全局详情页智能探测与 visited_pages 死循环拦截"""
        current_url = start_url
        page_count = 0
        total_items = 0
        seen_urls = set()
        consecutive_low_new = 0

        # 已访问翻页 URL 集合
        visited_pages = set()

        while current_url and self._running:
            page_count += 1
            self.log.emit(f"[翻页 | 探索] 正在请求第 {page_count} 页 -> {current_url}")

            visited_pages.add(current_url)

            response = self._make_request(current_url, rules, page=page_count)
            if not response:
                break

            html = response.text

            # =========================================================================
            # 🌟 智能防御：如果返回数据直接为 0（判定可疑，可能是静默指纹拦截），重刷本页
            # =========================================================================
            is_suspected_block = False
            temp_items = self._parse_json_response(html, current_url, rules) if rules.get(
                "response_type") == "json" else self._extract_items(html, current_url, rules)

            if page_count == 1 and len(temp_items) == 0:
                is_suspected_block = True
            elif len(temp_items) <= 3:
                block_keywords = [
                    'login', 'portal', '管理平台', 'oldsms', 'at0086', '管理系统',
                    '统一身份认证', '防火墙', '安全中心', '安全隔离', '访问验证',
                    '安全防护', '访问控制', '浏览器安全', '验证码', '防爬'
                ]
                if any(k in html.lower() for k in block_keywords):
                    is_suspected_block = True

            if is_suspected_block:
                self.log.emit(
                    f"[请求 | WAF拦截预警] 页面无有效数据，疑似遭到静默拦截。正在激活全局 scrapling 指纹锁重新请求...")

            # =========================================================================

            if rules.get("response_type") == "json":
                items = self._parse_json_response(html, current_url, rules)
            else:
                items = self._extract_items(html, current_url, rules)

            new_count = 0
            page_items = 0
            consecutive_old = 0
            should_stop_pagination = False

            for item in items:
                if not self._running:
                    break

                title = item.get('title', '').strip()
                link = item.get('url', '')
                date_text = item.get('date', '')

                if not title or len(title) < 6:
                    continue

                if link and link in seen_urls:
                    continue

                seen_urls.add(link)
                new_count += 1

                publish_date, days_old, is_valid_date = self._parse_date(date_text)

                # =========================================================================
                # 🌟 核心升级点（增强版）：如果列表页没有提供时间，或提供的时间不含 4 位年份
                # （如 11月17日），说明年份是程序猜测的。此时自动向详情页派发探测请求以校正年份。
                # =========================================================================
                # 检查原始列表页提取的文本中是否含有 19xx, 20xx, 21xx 这样的 4 位年份
                has_4digit_year = False
                if date_text:
                    if re.search(r'\b(19|20|21)\d{2}\b', str(date_text)):
                        has_4digit_year = True

                # 如果时间完全无效，或者时间有效但缺失年份（短日期），且存在详情页链接，则启动探测
                if (not is_valid_date or not has_4digit_year) and link:
                    self.log.emit(
                        f"[详情页探测] 列表时间缺失年份或不完整({date_text})。正在向详情页派发探测请求 -> {link}")
                    detail_date = self._probe_detail_page(link)
                    if detail_date:
                        # 只有详情页返回了包含完整年份的时间，才进行覆盖校正
                        if re.search(r'\b(19|20|21)\d{2}\b', str(detail_date)):
                            publish_date, days_old, is_valid_date = self._parse_date(detail_date)
                        else:
                            # 详情页也没拿到年份时的安全降级保障：维持原列表页的时间
                            temp_date, temp_days, temp_valid = self._parse_date(detail_date)
                            if temp_valid:
                                publish_date, days_old, is_valid_date = temp_date, temp_days, temp_valid

                if self.days > 0:
                    if not is_valid_date:
                        self.log.emit(
                            f"[过滤 | 异常] 无法解析原始时间文本 '{date_text}'，安全忽略该数据: {title[:20]}...")
                        continue

                    if days_old > self.days:
                        consecutive_old += 1
                        if consecutive_old >= 5:
                            self.log.emit(
                                f"[过滤 | 截止] 连续 {consecutive_old} 条公告超出设定时效 ({self.days}天)，执行提早终止")
                            should_stop_pagination = True
                            break
                        else:
                            self.log.emit(f"[过滤 | 忽略] 发布时间偏旧 ({days_old}天前)，继续向下扫描...")
                            continue
                    else:
                        consecutive_old = 0

                with self._lock:
                    self.total_collected += 1

                notice = Notice(
                    title=title,
                    url=link,
                    publish_date=publish_date,
                    school=school_name,
                    category=college_name,
                    days_old=days_old
                )

                if self._check_notice(notice):
                    self.notice_found.emit({
                        "title": notice.title,
                        "url": notice.url,
                        "publish_date": notice.publish_date,
                        "school": notice.school,
                        "category": notice.category,
                        "keywords": getattr(notice, 'keywords', ''),
                        "days_old": notice.days_old
                    })
                    self.log.emit(f"[采集 | 成功] 命中公告: 【{notice.title[:30]}...】 (日期: {notice.publish_date})")
                    with self._lock:
                        self.total_after_filter += 1
                    page_items += 1

            if should_stop_pagination:
                break

            total_items += page_items
            self.log.emit(f"[翻页 | 统计] 第 {page_count} 页解析完成: 本页扫描到 {new_count} 条，入库 {page_items} 条")

            # 核心容错优化：放宽为 0。仅在连续 2 页完全没有任何新数据时，才判定到达采集边界
            if new_count == 0:
                consecutive_low_new += 1
                if consecutive_low_new >= 2:
                    self.log.emit(f"[翻页 | 终止] 连续 {consecutive_low_new} 页无任何新增，判定已到采集边界，结束翻页")
                    break
            else:
                consecutive_low_new = 0

            next_url = self._get_next_page_url(html, current_url, rules)

            # 核心死循环防御
            if not next_url or next_url == current_url or next_url in visited_pages:
                self.log.emit(f"[翻页 | 结束] 未检测到下一页，或下一页为重复页面，正常结束翻页")
                break

            current_url = next_url

        return total_items
    # ====================== JSON 解析 ======================
    def _parse_json_response(self, html, base_url, rules):
        try:
            data = json.loads(html)
            items = []

            list_path = rules.get("json_list_key", "obj.content")
            current = data
            for key in list_path.split('.'):
                current = current.get(key, {}) if isinstance(current, dict) else current

            item_list = current if isinstance(current, list) else []

            title_field = rules.get("json_title", "stitle")
            date_field = rules.get("json_date", "dpostdate|dadddate")
            id_field = rules.get("json_id", "id")

            for item in item_list:
                if not isinstance(item, dict):
                    continue

                title = item.get(title_field) or item.get("title") or ""
                if len(str(title)) < 5:
                    continue

                news_id = item.get(id_field)
                full_url = ""
                if news_id:
                    full_url = f"https://smc.sjtu.edu.cn/news/detail/{news_id}"
                else:
                    for f in ["soutlink", "url", "link", "href"]:
                        if item.get(f):
                            full_url = urljoin(base_url, str(item.get(f)))
                            break

                date_text = ""
                for f in date_field.split('|'):
                    date_text = item.get(f)
                    if date_text:
                        break

                items.append({
                    "title": str(title).strip(),
                    "url": full_url,
                    "date": str(date_text).strip()
                })

            return items
        except Exception as e:
            self.log.emit(f"[JSON解析失败] {str(e)}")
            return []

    # ====================== 翻页判断 ======================
    def _get_next_page_url(self, html, current_url, rules=None):
        if 'page=' in current_url or 'api.php' in current_url:
            return self._handle_page_param_next_url(current_url)

        if rules and rules.get("response_type") == "json":
            try:
                data = json.loads(html)
                obj = data.get("obj", {})
                if obj.get("last") is False or obj.get("hasNext") is True:
                    return current_url
            except:
                pass

        return self._auto_detect_next_page(html, current_url)

    def _auto_detect_next_page(self, html, current_url):
        if 'api.php' in current_url:
            return None

        soup = BeautifulSoup(html, 'html.parser')
        parsed = urlparse(current_url)
        path = parsed.path

        # =========================================================================
        # 1. 新增：JS 动态渲染分页函数逆向模拟保障
        # =========================================================================
        # 匹配诸如: createPageHTML(19, 1, "index","shtml","109")
        js_page_match = re.search(
            r"createPageHTML\(\s*(\d+)\s*,\s*(\d+)\s*,\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]",
            html
        )
        if js_page_match:
            try:
                total_pages = int(js_page_match.group(1))
                current_page = int(js_page_match.group(2))
                page_name = js_page_match.group(3)
                page_ext = js_page_match.group(4)

                # JS 内部控制下一页逻辑：page < total - 1 时触发下一页文件名拼装
                if current_page < total_pages - 1:
                    next_page_num = current_page + 1
                    next_page_file = f"{page_name}_{next_page_num}.{page_ext}"

                    # 替换当前 URL 路径中的文件名
                    path_parts = parsed.path.split('/')
                    path_parts[-1] = next_page_file
                    new_path = '/'.join(path_parts)

                    next_url = f"{parsed.scheme}://{parsed.netloc}{new_path}"
                    if next_url != current_url:
                        self.log.emit(f"[翻页 | JS仿真] 成功识读脚本调用，模拟渲染出下一页 -> {next_url}")
                        return next_url
            except Exception as e:
                self.log.emit(f"[翻页 | 警告] 逆向解析动态翻页脚本异常: {e}")

        # =========================================================================
        # 2. 经典类名精确匹配（非动态渲染型页面）
        # =========================================================================
        next_selectors = [
            'a.to_right', 'a.next', 'a.next-page', '.next a', '.pages-next a',
            'a[class*="next"]', 'a[class*="to_right"]'
        ]
        for sel in next_selectors:
            next_a = soup.select_one(sel)
            if next_a and next_a.get('href'):
                href = next_a.get('href', '').strip()
                if href and not href.startswith(('javascript:', '#')):
                    next_url = urljoin(current_url, href)
                    if next_url != current_url:
                        self.log.emit(f"[翻页 | 精确] 成功通过类名 [{sel}] 捕获到下一页 -> {next_url}")
                        return next_url

        # =========================================================================
        # 3. 使用 autopager 语义模型进行关系寻址
        # =========================================================================
        try:
            import autopager
            for link, rel in autopager.extract(html):
                if rel == 'next' and link and link.get('href'):
                    href = link.get('href', '').strip()
                    if href and not href.startswith(('javascript:', '#')):
                        next_url = urljoin(current_url, href)
                        if next_url != current_url:
                            self.log.emit(f"[翻页 | autopager] 语义模型智能识别出下一页链接 -> {next_url}")
                            return next_url
        except ImportError:
            pass
        except Exception as e:
            self.log.emit(f"[翻页 | 警告] autopager 运行异常: {e}")

        # =========================================================================
        # 4. 备用通道一：基于文本模糊匹配
        # =========================================================================
        for txt in ['下一页', '下一頁', '下页', '后一页', '›', '>']:
            a = soup.find('a', string=lambda s: s and txt in str(s))
            if a and a.get('href'):
                next_url = urljoin(current_url, a.get('href'))
                if next_url != current_url:
                    return next_url

        # =========================================================================
        # 5. 备用通道二：基于 URL 增量规则自动推算
        # =========================================================================
        patterns = [
            (r'/p?(\d+)\.html?$', lambda m: f"/p{int(m.group(1)) + 1}.html"),
            (r'list(\d*)\.htm?$', lambda m: f"list{int(m.group(1) or 0) + 1}.htm"),
            (r'/(\d+)\.htm$', lambda m: f"/{int(m.group(1)) + 1}.htm"),
            (r'tzgg/(\d+)\.htm', lambda m: f"tzgg/{int(m.group(1)) + 1}.htm"),
        ]

        for pattern, replacer in patterns:
            match = re.search(pattern, path)
            if match:
                try:
                    new_path = re.sub(pattern, replacer(match), path)
                    next_url = f"{parsed.scheme}://{parsed.netloc}{new_path}"
                    if next_url != current_url:
                        return next_url
                except:
                    continue
        return None

    def _handle_page_param_next_url(self, current_url):
        parsed = urlparse(current_url)
        query_params = dict(x.split('=') for x in parsed.query.split('&') if '=' in x)

        if 'page' in query_params:
            try:
                current_page = int(query_params['page'])
                next_page = current_page + 1
                query_params['page'] = str(next_page)
                new_query = '&'.join([f"{k}={v}" for k, v in query_params.items()])
                return f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{new_query}"
            except:
                pass

        if 'page' not in query_params:
            return current_url + ('&' if '?' in current_url else '?') + 'page=2'
        return None

    # ====================== 核心提取机制 ======================
    def _extract_items(self, html, base_url, rules):
        """核心提取入口"""
        # =========================================================================
        # 🌟 换个角度：针对辽宁师范大学等分离卡片排版，进行底层原始 HTML 特征流逆向扫描
        # =========================================================================
        if 'class="date"' in html and 'class="item"' in html and '查看详情' in html:
            items = []
            # 正则直接穿透复杂 DOM 层级，精准一次性抓取标题、链接、日、年、月
            pattern = re.compile(
                r'<h2>(.*?)</h2>.*?href=["\'](.*?)["\'].*?<div class="date"><span>(\d{1,2})</span>/(\d{4})-(\d{2})</div>',
                re.DOTALL
            )
            matches = pattern.findall(html)
            for m in matches:
                title = re.sub(r'<[^>]+>', '', m[0]).strip()  # 剥离可能残存的 html 标签
                url = urljoin(base_url, m[1].strip())
                day = m[2].zfill(2)
                year = m[3]
                month = m[4].zfill(2)
                date_str = f"{year}-{month}-{day}"  # 直接在底层拼装出标准格式，杜绝漂移

                items.append({
                    "title": title,
                    "url": url,
                    "date": date_str
                })

            if items:
                self.log.emit(f"[智能拦截 | 成功] 物理特征扫描匹配成功，直接穿透捕获到 {len(items)} 条完整公告！")
                return items
        # =========================================================================

        # 如果非辽师大专属排版，走常规解析逻辑
        if rules.get("title_rule"):
            return self._parse_with_rules(html, base_url, rules)
        else:
            return self._auto_detect_parse(html, base_url)

    def _parse_with_rules(self, html, base_url, rules):
        title_type = rules.get("title_type", "xpath")
        if title_type == "xpath":
            return self._parse_xpath(html, base_url, rules.get("title_rule"), rules.get("link_rule"),
                                     rules.get("date_rule"))
        elif title_type == "css":
            return self._parse_css(html, base_url, rules.get("title_rule"), rules.get("link_rule"),
                                   rules.get("date_rule"))
        elif title_type == "regex":
            return self._parse_regex(html, base_url, rules.get("title_rule"), rules.get("link_rule"),
                                     rules.get("date_rule"))
        return []

    def _parse_xpath(self, html, base_url, title_rule, link_rule, date_rule):
        items = []
        try:
            tree = etree.HTML(html)
            titles = tree.xpath(title_rule) if title_rule else []
            links = []
            if link_rule:
                link_elems = tree.xpath(link_rule)
                for elem in link_elems:
                    href = elem if isinstance(elem, str) else (elem.get('href', '') if hasattr(elem, 'get') else '')
                    if href:
                        links.append(urljoin(base_url, href))

            dates = []
            if date_rule:
                date_elems = tree.xpath(date_rule)
                for elem in date_elems:
                    if isinstance(elem, str):
                        dates.append(elem.strip())
                    elif hasattr(elem, 'text'):
                        dates.append((elem.text or "").strip())
                    elif hasattr(elem, 'xpath'):
                        dates.append((elem.xpath('string(.)') or "").strip())

            for i, title_elem in enumerate(titles):
                title = title_elem.strip() if isinstance(title_elem, str) else (
                    (title_elem.text or "").strip() if hasattr(title_elem, 'text') else "")
                url = links[i] if i < len(links) else ""
                if not url and hasattr(title_elem, 'get'):
                    href = title_elem.get('href', '')
                    if href:
                        url = urljoin(base_url, href)

                date_text = dates[i] if i < len(dates) else ""
                if title:
                    items.append({"title": title, "url": url, "date": date_text})
        except Exception as e:
            self.log.emit(f"[规则 | 异常] 用户XPath规则执行失败: {str(e)}")
        return items

    def _parse_css(self, html, base_url, title_rule, link_rule, date_rule):
        items = []
        try:
            soup = BeautifulSoup(html, 'html.parser')
            title_elems = soup.select(title_rule) if title_rule else []
            links = []
            if link_rule:
                link_elems = soup.select(link_rule)
                for elem in link_elems:
                    if elem.get('href'):
                        links.append(urljoin(base_url, elem.get('href')))

            for i, title_elem in enumerate(title_elems):
                title = title_elem.get_text(strip=True)
                url = links[i] if i < len(links) else ""
                if not url:
                    a = title_elem if title_elem.name == 'a' else title_elem.find('a')
                    if a and a.get('href'):
                        url = urljoin(base_url, a.get('href'))

                date_text = ""
                if date_rule:
                    parent = title_elem.parent
                    if parent:
                        date_elem = parent.select_one(date_rule)
                        if date_elem:
                            date_text = date_elem.get_text(strip=True)

                if title:
                    items.append({"title": title, "url": url, "date": date_text})
        except Exception as e:
            self.log.emit(f"[规则 | 异常] 用户CSS规则执行失败: {str(e)}")
        return items

    def _parse_regex(self, html, base_url, title_rule, link_rule, date_rule):
        items = []
        try:
            title_matches = re.findall(title_rule, html) if title_rule else []
            link_matches = re.findall(link_rule, html) if link_rule else []
            date_matches = re.findall(date_rule, html) if date_rule else []

            for i, title in enumerate(title_matches):
                if isinstance(title, tuple):
                    title = title[0] if title else ""
                title = re.sub(r'<[^>]+>', '', str(title)).strip()

                url = ""
                if i < len(link_matches):
                    link = link_matches[i]
                    if isinstance(link, tuple):
                        link = link[0] if link else ""
                    url = urljoin(base_url, str(link))

                date_text = ""
                if i < len(date_matches):
                    d = date_matches[i]
                    if isinstance(d, tuple):
                        d = d[0] if d else ""
                    date_text = re.sub(r'<[^>]+>', '', str(d)).strip()

                if title:
                    items.append({"title": title, "url": url, "date": date_text})
        except Exception as e:
            self.log.emit(f"[规则 | 异常] 用户正则规则执行失败: {str(e)}")
        return items

    # ====================== 自动识别重构核心 ======================
    def _auto_detect_parse(self, html, base_url):
        """三级分流自适应提取引擎（极速自愈版：Script-JSON 降维打击置顶）"""
        self.log.emit(f"[自动识别] 启动解析: {base_url}")

        # =========================================================================
        # 🌟 核心升级点：将 Script-JSON 提升到【第一绝对优先级】
        # 如果页面内嵌了 CMS 数据库源（如 dataList, allData），它代表最真实、最完整的公告列表，直接降维提取，杜绝任何 DOM 噪音
        # =========================================================================
        items = self._parse_script_json_embeds(html, base_url)
        if items and len(items) >= 3:
            return items

        # =========================================================================
        # 🌟 第二级优先级：开发者明确编写的特异性手工卡片规则配置（100% 精准）
        # =========================================================================
        items = self._parse_handcrafted_layouts(html, base_url)
        if items and len(items) >= 1:
            return items

        # =========================================================================
        # 🌟 第三级优先级：Gerapy 自动化列表兜底
        # =========================================================================
        items = self._extract_via_gerapy_alignment(html, base_url)
        if items and len(items) >= 3:
            return items

        # =========================================================================
        # 🌟 第四级优先级：无结构化纯链接兜底
        # =========================================================================
        items = self._extract_via_raw_links_fallback(html, base_url)
        return items

    def _extract_via_gerapy_alignment(self, html, base_url):
        """
        Gerapy 骨架提取 + DOM 邻近时间对齐 Pipeline
        """
        try:
            from gerapy_auto_extractor import extract_list
            list_data = extract_list(html)
            gerapy_items = list_data.get('items', []) if isinstance(list_data, dict) else (list_data or [])

            if not gerapy_items or len(gerapy_items) < 3:
                return []

            soup = BeautifulSoup(html, 'html.parser')
            aligned_items = []

            for item in gerapy_items:
                title = (item.get('title') or '').strip()
                link = (item.get('url') or item.get('link') or '').strip()

                if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                    continue
                if not link or any(p in link.lower() for p in ['javascript:', '#', 'mailto:']):
                    continue

                full_url = urljoin(base_url, link)
                if not any(full_url.lower().startswith(p) for p in ['http://', 'https://']):
                    continue

                # 提取初始日期
                date_text = (item.get('date') or item.get('time') or '').strip()
                is_complete = bool(date_text and (
                        re.search(r'\d{4}[-/年.]\d{1,2}[-/月.]\d{1,2}', date_text) or re.match(r'^\d{8}$',
                                                                                               date_text)))

                # DOM 邻近寻址对齐
                if not is_complete:
                    a_tag = self._find_link_node(soup, base_url, link)
                    if a_tag:
                        container = self._find_single_row_container(a_tag, base_url)
                        # 使用通用微合流提取器
                        reconstructed = self._extract_split_date_from_container(container, title)
                        if reconstructed:
                            date_text = reconstructed
                            is_complete = True
                        else:
                            date_text = self._extract_date_from_element(container) or date_text

                # 清洗标题中可能附带的时间噪音
                if date_text and date_text in title:
                    title = title.replace(date_text, "").strip()
                title = title.strip('- \t\n\r|｜[]()（）')

                # 解析日期
                formatted_date, days_old, success = self._parse_date(date_text)

                # 详情页网络探测探测（若列表页无有效日期且存在详情页链接，执行轻量探测）
                if not success and full_url:
                    detail_date = self._probe_detail_page(full_url)
                    if detail_date:
                        formatted_date, days_old, success = self._parse_date(detail_date)

                aligned_items.append({
                    "title": title,
                    "url": full_url,
                    "date": formatted_date if success else date_text,
                    "days_old": days_old
                })

            return aligned_items
        except ImportError:
            self.log.emit("[自动识别] gerapy_auto_extractor 未安装，切换下一层探测...")
        except Exception as e:
            self.log.emit(f"[自动识别 | 异常] Gerapy 自动寻址分支异常: {e}")
        return []

    # ====================== 脚本JSON解析分支 ======================
    def _parse_script_json_embeds(self, html, base_url):
        """解析页面 Script 标签中嵌入的 JSON 列表（升级版：支持非严格控制字符译码）"""
        soup = BeautifulSoup(html, 'html.parser')
        script_tags = soup.find_all('script')
        current = []

        for script in script_tags:
            script_text = script.get_text()
            if 'dataList' in script_text:
                start_idx = script_text.find('dataList')
                bracket_start = script_text.find('[', start_idx)
                if bracket_start != -1:
                    bracket_count, bracket_end = 0, -1
                    for i in range(bracket_start, len(script_text)):
                        if script_text[i] == '[':
                            bracket_count += 1
                        elif script_text[i] == ']':
                            bracket_count -= 1
                            if bracket_count == 0:
                                bracket_end = i
                                break
                    if bracket_end != -1:
                        try:
                            import json
                            json_str = script_text[bracket_start:bracket_end + 1]
                            # 🌟 核心升级点：采用 strict=False 宽容模式，允许 JSON 字符串中含有未转义的 \n \t \r 等控制字符
                            data_list = json.loads(json_str, strict=False)
                            for block in data_list:
                                for info in block.get('infolist', []):
                                    title = (info.get('title') or info.get('infotitle') or '').strip()
                                    link = (info.get('url') or info.get('sourceurl') or '').strip()
                                    if len(title) < 4:
                                        continue

                                    # 提取时间
                                    date_text = (info.get('daytime') or '').strip()
                                    if not date_text:
                                        ts = info.get('releaseTime') or info.get('releasetime') or info.get(
                                            'createtime')
                                        if ts:
                                            try:
                                                dt_obj = datetime.fromtimestamp(
                                                    int(ts) / 1000 if len(str(ts)) >= 13 else int(ts))
                                                date_text = dt_obj.strftime('%Y-%m-%d')
                                            except:
                                                pass
                                    current.append({"title": title, "url": urljoin(base_url, link), "date": date_text})
                        except Exception as ex:
                            # 打印异常诊断日志，排除其他潜在干扰
                            self.log.emit(f"[自动识别 | 脚本异常] 解析 dataList 脚本 JSON 数据失败: {ex}")

            # 2. 匹配 allData 模式
            elif 'allData' in script_text or 'new graduate' in script_text:
                blocks = re.findall(r'\{[^{}]*?\}', script_text, re.DOTALL)
                for block in blocks:
                    if re.search(r'["\']?url["\']?\s*:', block) and re.search(r'["\']?date["\']?\s*:', block):
                        try:
                            url_match = re.search(r'["\']?url["\']?\s*:\s*["\'](.*?)["\']', block)
                            date_match = re.search(r'["\']?date["\']?\s*:\s*["\'](.*?)["\']', block)
                            title_match = re.search(r'["\']?(text|title)["\']?\s*:\s*["\'](.*?)["\']', block)
                            if url_match and date_match and title_match:
                                current.append({
                                    "title": title_match.group(2).strip(),
                                    "url": urljoin(base_url, url_match.group(1).strip()),
                                    "date": date_match.group(1).strip()
                                })
                        except:
                            continue

        if len(current) >= 3:
            self._recalc_current_dates(current)
            self.log.emit(f"[自动识别 | 脚本] 发现隐藏在 <script> 中的 JSON 数据列表，共提取 {len(current)} 条")
            return current
        return []

    # ====================== 经典 DOM 结构自适应重构 ======================
    def _extract_date_from_url(self, url):
        """从公告 URL 路径中智能提取发布日期（升级版：含 1990~2050 年份防污染保护锁）"""
        if not url:
            return None

        # 模式 A：/YYYY/MMDD/ 格式（最经典的 VSB 静态文件路径，如 /2026/0526/）
        m_vsb_a = re.search(r'/(\d{4})/(\d{2})(\d{2})/', url)
        if m_vsb_a:
            year = m_vsb_a.group(1)
            month = m_vsb_a.group(2)
            day = m_vsb_a.group(3)
            try:
                # 🌟 核心防污染锁：只有当年份符合 1990~2050 客观现实时才予采信，彻底屏蔽栏目ID干扰
                if 1990 <= int(year) <= 2050 and 1 <= int(month) <= 12 and 1 <= int(day) <= 31:
                    return f"{year}-{month}-{day}"
            except:
                pass

        # 模式 B：/YYYY/MM/DD/ 格式（如 /2026/05/26/ 或 /2026/5/6/）
        m_vsb_b = re.search(r'/(\d{4})/(\d{1,2})/(\d{1,2})/', url)
        if m_vsb_b:
            year = m_vsb_b.group(1)
            month = m_vsb_b.group(2).zfill(2)
            day = m_vsb_b.group(3).zfill(2)
            try:
                if 1990 <= int(year) <= 2050 and 1 <= int(month) <= 12 and 1 <= int(day) <= 31:
                    return f"{year}-{month}-{day}"
            except:
                pass

        # 模式 C：/YYYY-MM-DD/ 格式（常见于某些现代化 CMS 路径）
        m_vsb_c = re.search(r'/(\d{4})-(\d{1,2})-(\d{1,2})/', url)
        if m_vsb_c:
            year = m_vsb_c.group(1)
            month = m_vsb_c.group(2).zfill(2)
            day = m_vsb_c.group(3).zfill(2)
            try:
                if 1990 <= int(year) <= 2050 and 1 <= int(month) <= 12 and 1 <= int(day) <= 31:
                    return f"{year}-{month}-{day}"
            except:
                pass

        return None

    def _parse_handcrafted_layouts(self, html, base_url):
        """通用自适应多卡片布局解析器（终极自愈版：支持 link_sel 优先级重写与递归过滤）"""
        soup = BeautifulSoup(html, 'html.parser')
        current = []

        # =========================================================================
        # 🌟 拦截器 1：优先检测是否存在无 A 标签的 .new_every (window.open) 动态卡片
        # =========================================================================
        new_every_nodes = soup.select('.new_every')
        if new_every_nodes:
            for node in new_every_nodes:
                desc_p = node.select_one('p.news_desc')
                if not desc_p:
                    continue
                title = desc_p.get_text(strip=True)
                title = re.sub(r'[…\s\.]+$', '', title).strip()

                if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                    continue

                # 从内层 p 的 onclick 属性中解析跳转链接
                onclick = desc_p.get('onclick', '') or desc_p.get('onClick', '') or ""
                url = ""
                match = re.search(r"window\.open\(\s*['\"](.*?)['\"]", onclick)
                if match:
                    url = urljoin(base_url, match.group(1).strip())

                if not url:
                    continue

                # 提取并拼合日期
                date_text = ""
                time_p = node.select_one('.news_time')
                if time_p:
                    spans = time_p.find_all('span')
                    if len(spans) >= 2:
                        date_text = spans[1].get_text(strip=True)
                    else:
                        date_text = time_p.get_text(strip=True)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 1:
                self._recalc_current_dates(current)
                self.log.emit(
                    f"[自动识别 | 动态卡片] 成功匹配 .new_every (window.open) 布局 -> 共提取 {len(current)} 条数据")
                return current

        # =========================================================================
        # 🌟 拦截器 1.5：全能型 JS 跳转事件语义破译引擎（ li[onclick / onClick] 通用对齐）
        # =========================================================================
        onclick_list_items = []
        for li in soup.find_all('li'):
            onclick_val = ""
            for attr_name, attr_val in li.attrs.items():
                if attr_name.lower() == 'onclick':
                    onclick_val = str(attr_val).strip()
                    break

            if onclick_val and any(k in onclick_val for k in ['location', 'window.location.href', 'window.location']):
                title_node = li.select_one('.title, h5, .blog-rt-box-item-lf span, span')
                date_node = li.select_one('.time, .blog-rt-box-item-rt, span.time, .date')
                if title_node and date_node:
                    onclick_list_items.append((li, title_node, date_node, onclick_val))

        if onclick_list_items:
            for li, t_node, d_node, onclick in onclick_list_items:
                title = t_node.get_text(strip=True)
                title = re.sub(r'[…\s\.]+$', '', title).strip()

                if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = ""
                match = re.search(r"(?:window\.location\.href|window\.location|location)\s*=\s*['\"](.*?)['\"]",
                                  onclick)
                if match:
                    url = urljoin(base_url, match.group(1).strip())

                if not url:
                    continue

                date_text = d_node.get_text(strip=True)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 1:
                self._recalc_current_dates(current)
                self.log.emit(
                    f"[自动识别 | 动态卡片] 成功程序化破译 li[onclick] 跳转行为 -> 共提取 {len(current)} 条数据")
                return current

        # =========================================================================
        # 🌟 拦截器 1.6：针对海南大学等超非标、标签残缺的 .m_new13 (缺失 LI 标签) 列表
        # =========================================================================
        m_new13_box = soup.select_one('.m_new13, div.m_new13')
        if m_new13_box:
            for fat in m_new13_box.find_all('fat'):
                a = fat.find('a', href=True)
                if not a:
                    continue

                # 1. 优先提取 100% 完整 title
                title = a.get('title', '').strip() or a.get_text(strip=True)
                title = re.sub(r'[…\s\.]+$', '', title).strip()

                if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', '').strip())

                # 2. 🌟 核心：利用相邻兄弟节点指针，横向搜寻同级相邻的下一个 <span> 作为时间
                date_text = ""
                sibling = fat.find_next_sibling()
                while sibling:
                    if sibling.name == 'span':
                        date_text = sibling.get_text(strip=True)
                        break
                    sibling = sibling.find_next_sibling()

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 1:
                self._recalc_current_dates(current)
                self.log.emit(
                    f"[自动识别 | 动态卡片] 成功破译残缺 .m_new13 (缺失LI标签) 排版 -> 共提取 {len(current)} 条数据")
                return current

        # =========================================================================
        # 🌟 拦截器 2：自适应卡片规则配置区（高精度在前，宽泛在后，追加 min_count 参数）
        # =========================================================================
        configs = [
            # ==================== 新增：云南师大等文字列表（.text-list li 复合卡片） ====================
            {
                "name": "云南师大等文字列表（.text-list li 复合卡片）",
                "selector": "div.text-list li, .text-list li",
                "title_sel": "h3.titline, .titline",  # 精准锁定标题 h3
                "link_sel": "a",  # 锁定链接 a
                "date_sel": "time, .time time",  # 🌟 强行锁定 time 标签，隔离右侧简介段落的数字噪音
                "min_count": 1
            },
            # ==================== 新增：辽宁师范大学等卡片列表（.item 双层分离结构） ====================
            {
                "name": "辽宁师范大学等卡片列表（.item 双层分离结构）",
                "selector": "div.item, .item",
                "title_sel": "h2",  # 🌟 精准锁定 h2 提取真实标题
                "link_sel": "a[href]",  # 🌟 锁定同级的 a 标签提取跳转链接
                "date_sel": "div.date, .date",  # 锁定日期容器
                "min_count": 1  # 允许单条或少量公告时也强力适配，防止漏抓
            },
            # ==================== 新增：宁波大学等 WordPress 列表（.inner-post .entry-header 结构） ====================
            {
                "name": "宁波大学等 WordPress 列表（.inner-post .entry-header 结构）",
                "selector": ".inner-post .entry-header, div.inner-post > div.entry-header",
                # 🌟 显式指定 .entry-header 为行节点
                "title_sel": "h4 a",  # 锁定 h4 下的 a 为标题
                "link_sel": "h4 a",  # 锁定 h4 下的 a 为链接
                "date_sel": "time.entry-date, .posted-on time",  # 锁定 time 标签为日期
                "min_count": 1
            },
            # ==================== 新增：成都中医药大学等图文列表（.pageTPList li 复合结构） ====================
            {
                "name": "成都中医药大学等图文列表（.pageTPList li 复合结构）",
                "selector": "ul.pageTPList li, .pageTPList li",
                "title_sel": "a.tit",
                "link_sel": "a.tit",
                "date_sel": "span.date, .date",  # 🌟 显式精准锁定 span.date，强行绕过 [通知公告] 干扰标签
                "min_count": 1
            },
            # ==================== 新增：山东大学等 VSB 列表（.txtList li 复合卡片） ====================
            {
                "name": "山东大学等 VSB 列表（.txtList li 复合卡片）",
                "selector": "ul.txtList li, .txtList li",
                "title_sel": "h4",
                "link_sel": "a",
                "date_sel": ".times, time",
                "min_count": 1
            },
            {
                "name": "清华大学研招网类图文列表（.admission 复合自关联卡片）",
                "selector": "div.admission ul li, .admission ul li",  # 🌟 严格限制在外层容器的 ul li 内，隔离非公告节点
                "title_sel": "a",  # 在 li 内部寻找 a 标签作为标题
                "link_sel": "a",  # 在 li 内部寻找 a 标签作为链接
                "date_sel": ".time",  # 锁定 .time 作为日期
                "min_count": 1
            },
            # ==================== 新增：南京理工等 VSB 带有阅读量与 P 标签日期布局 ====================
            {
                "name": "南京理工等 VSB 带有阅读量与 P 标签日期布局",
                "selector": "ul.news_ul > li, .news_ul > li, li:has(span[class*='VisitCount'])",
                "title_sel": "a",
                "link_sel": "a",
                "date_sel": "p",  # 🌟 精准锁定 p 作为真实日期容器
                "min_count": 1
            },
            {
                "name": "东华大学研究生院公告列表（ul.news_list > li.news 结构）",
                "selector": "ul.news_list > li.news",
                # 🌟 每条公告在 li.news 内，保证能命中全部条目
                "title_sel": ".news_title",
                # 🌟 标题在 div.news_title 内
                "link_sel": "a.news_box",
                # 🌟 链接就是外层 a.news_box 的 href
                "date_sel": ".news_meta",
                # 🌟 日期由 .news_meta 内的 span.news_day + span.news_year 组成，交给清洗器拼合
                "min_count": 3
            }
,
            {
                "name": "西南大学研究生院通知列表（div.list > ul > li 极简结构）",
                "selector": "div.list > ul > li",
                "title_sel": "span.tit",
                # 标题在 <span class="tit"> 内
                "link_sel": "a[target='_blank']",
                # 链接取 a 的 href
                "date_sel": "p.time",
                # 日期在 <p class="time"> 内，包含 span 和年份文本
                "min_count": 3
            }

            ,
        {
                "name": "西安工程大学通知公告（line_u + span日期）",
                "selector": "ul.ss li[id^='line_u']",
                "title_sel": "a",
                "link_sel": "a",
                "title_attr": "title",
                "date_sel": "span[style*='width:19%']",
                "min_count": 3
        }
            ,
            {
                "name": "四川大学艺术学院 slideBox 新闻列表",
                "selector": "div.slideBox ul li, .slideBox UL li",
                "title_sel": "a.fontYY",
                "link_sel": "a.fontYY",
                "title_attr": "text",  # 改成 text 提取文本内容
                "date_sel": "",  # 页面没有日期字段，留空
                "min_count": 3
            }
            ,
            {
                "name": "中原工学院类图文卡片（ul.l2-q + .date 复合结构）",
                "selector": "ul.l2-q li, .l2-q li",
                "title_sel": "h4",  # 精准锁定 h4 作为标题，防止标题被 date 标签及 a 的全文本污染
                "link_sel": "a",
                "date_sel": ".date",  # 锁定 date 容器，由底层时间清洗器自动拼合 d1(年) 与 d2(月-日)
                "min_count": 1
            },
            {
                "name": "清华安全学院类图文卡片（.tz-list + .tz-list-tt 复合结构）",
                "selector": "ul.tz-list li, .tz-list li",
                "title_sel": "",  # 留空以优先提取 <a> 标签中 100% 完整的 title 属性值
                "link_sel": "a",
                "date_sel": ".tz-list-date",  # 锁定 tz-list-date 容器，由底层重构后的时间解析器进行秒级破译
                "min_count": 1
            },
            {
                "name": "清华生命科院类图文列表（li.withimg + .time.motion 复合结构）",
                "selector": "li.withimg, .withimg",
                "title_sel": "h3",
                "link_sel": "a",
                "date_sel": ".time.motion, .time",
                "min_count": 1
            },
            {
                "name": "浙大高分子系类图文卡片（.list-item + .date 复合结构）",
                "selector": "li.list-item, .list-item",
                "title_sel": "h3",
                "link_sel": "a",
                "date_sel": ".date",  # 锁定 date 容器，由底层的二位数年份补全器与模式 D 自动重组
                "min_count": 1
            },
            {
                "name": "首师大心理学院类图文卡片（.page-list33 + .gpArticleTitle 复合结构）",
                "selector": ".page-list33 li, ul.block-list li",
                "title_sel": ".gpArticleTitle",
                "link_sel": "a",
                "date_sel": ".gpArticleDate",  # 锁定 gpArticleDate，由底层对齐器自动拼合 .day(月.日) 与 .month(年)
                "min_count": 1
            },
            # ==================== 替换/升级：VSB/GPower 通用新闻列表（news_list + news_meta 结构） ====================
            {
                "name": "VSB与GPower系统通用通知列表（news_list + news_meta 结构）",
                "selector": "ul.news_list li.news, .news_list li.news, ul.news_list li",
                # 🌟 彻底抛弃 wp_news_wX 动态ID，直接锁定类名
                "title_sel": "span.news_title a, .news_title a",  # 精准锁定标题 a
                "link_sel": "span.news_title a, .news_title a",  # 精准锁定链接 a
                "date_sel": "span.news_meta, .news_meta",  # 🌟 显式指定 span.news_meta，完美避开前面的标题 span
                "min_count": 1
            },
            # {
            #     "name": "长安大学运输工程学院通知列表（wp_news_w6 + news_list 双类名结构）",
            #     "selector": "div#wp_news_w6 ul.news_list > li.news",
            #     # 🌟 wp_news_w6 是 GPower 通知窗口专属 id，news_list 是 ul 列表类；li.news 双类名精准命中
            #     "title_sel": "span.news_title a[target='_blank']",
            #     # 🌟 标题在 span.news_title 内的 a；a 带 target="_blank"，排除导航
            #     "link_sel": "span.news_title a[target='_blank']",
            #     # 🌟 selector 命中 <li> 不是链接节点；链接在 span.news_title 内的 a 上
            #     "date_sel": "span.news_meta",
            #     # 🌟 日期容器是 span.news_meta，专属类名
            #     "min_count": 1
            # }

            {
                "name": "重大生命科院类图文卡片（.wzlb + .text h4 a 复合结构）",
                "selector": ".wzlb li, .wps li",
                "title_sel": ".text h4 a",
                "link_sel": "a",
                "date_sel": ".time",  # 锁定 time 容器，由底层时间清洗器自动拼合 h4(日) 与 p(年月)
                "min_count": 1
            },
            {
                "name": "重庆大学某学院通知列表（article.news-item 卡片式 双段日期）",
                "selector": "article.news-item",
                # 🌟 article 是 HTML5 语义化标签，news-item 是专属类名，精准命中
                "title_sel": "h3",
                # 🌟 标题在 .news-info 内的 h3；h3 在 article 内唯一
                "link_sel": "a.read-more",
                # 🌟 链接 a 自带 read-more 类名；article 内唯一链接
                "date_sel": "div.list-thumb",
                # 🌟 日期容器是 .list-thumb，含 h2(日) + h4(年月) 两段；需在抓取层拼成 YYYY-MM-DD
                "min_count": 1
            },
            {
                "name": "西安电子科技大学研究生院通知列表（ul.list1 + date1 双段日期结构）",
                "selector": "ul.list1 > li",
                # 🌟 list1 是 ul 专属类名，> li 直接子选择器精准命中
                "title_sel": "p.title.tt1",
                # 🌟 标题在 .text 内的 p.title.tt1；双类名 title + tt1 与描述区 .info.tt1 不撞
                "link_sel": "a[href]",
                # 🌟 selector 命中 <li> 不是链接节点；li 内整个内容被 a 包裹，a 是 li 内唯一链接
                "date_sel": "div.date1",
                # 🌟 日期容器是 .date1，含 day + 年月 两段；需在抓取层拼成 YYYY-MM-DD
                "min_count": 1
            }
            ,
            {
                "name": "西南交大电气工程学院通知公告列表（div.item flex 布局 月日日期）",
                "selector": "div.item",
                # 🌟 item 是条目专属类名；不会撞到 section 分隔线（分隔线是 span 不是 div）
                "title_sel": "p.a_block.line1",
                # 🌟 标题在 .a_block.line1 内；.a_block + .line1 双类名精准唯一
                "link_sel": "a[href]",
                # 🌟 selector 命中 div.item 不是链接节点；item 内整个内容被 a 包裹，a 是 item 内唯一链接
                "date_sel": "div.date",
                # 🌟 日期容器是 .date，专属类名；只含月日如 "10-24"，需在抓取层补年份
                "min_count": 1
            },
            {
                "name": "通用 VSB 系统嵌套表格样式（table.lists 经典变体）",
                "selector": "table.lists table tr, .lists table tr",
                "title_sel": "a",
                "link_sel": "a",
                "date_sel": "td[align='right'], td.zym",  # 锁定 align='right' 或 zym 节点作为时间来源
                "min_count": 1
            },
            {
                "name": "石河子大学水利院类文字卡片（.jzlb + .fbsj4 复合结构）",
                "selector": "div.jzlb, .jzlb",
                "title_sel": ".btt3 a",
                "link_sel": "a",
                "date_sel": ".fbsj4",  # 锁定 fbsj4 节点作为时间来源
                "min_count": 1
            },
            {
                "name": "海南大学国际商学院通知列表（ax-grid + dl伪表格 三段结构）",
                "selector": "ul.ax-grid > li",
                # 🌟 ax-grid 是响应式栅格专属类，> li 直接子选择器精准命中
                "title_sel": "dl span.text12 b",
                # 🌟 标题在 .text12 内的 b 标签；b 在 li 内是唯一的"加粗标题"
                "link_sel": "a[target='_blank']",
                # 🌟 链接 a 自带 target="_blank" 属性；title 属性也带 target，selector 取 a 自身即链接
                "date_sel": "dl span.text10",
                # 🌟 完整日期在 .text10 中（2026.05.08），无需拼接
                "min_count": 1
            },
            {
                "name": "河北师大类 WordPress 文章卡片（article + .entry-title 结构）",
                "selector": "article.type-post, article",  # 🌟 直接锁定 HTML5 的文章主体 article 容器，完美穿透各种师大自建的 WP 系统
                "title_sel": ".entry-title a",
                "link_sel": "a",
                "date_sel": ".entry-title span",  # 显式锁定标题行右侧的 span 标签作为时间来源
                "min_count": 1
            },
            {
                "name": "GPower CMS 招生通知列表（sub_014 + li > a.time/.title 双段结构）",
                "selector": "div.sub_014 > ul > li",
                # 🌟 sub_014 是 GPower 列表专属容器，> ul > li 精准命中条目
                "title_sel": "div.title",
                # 🌟 标题在 a 内的 .title；div.title 在 li 内唯一
                "link_sel": "a[href]",
                # 🌟 selector 命中 <li> 不是链接节点；li 内整个内容被 a 包裹，a 是 li 内唯一链接
                "date_sel": "div.time",
                # 🌟 日期在 a 内的 .time；div.time 在 li 内唯一
                "min_count": 1
            }
            ,
            {
                "name": "延边大学研究生招生通知列表（div.entry 响应式双列布局）",
                "selector": "div.entry",
                # 🌟 entry 是条目专属类名；不加 ul/li 外壳限制，entry 自身就是条目节点
                "title_sel": "div.entry-title h4 a",
                # 🌟 标题在左列 h4 > a 里；h4 + a 双层结构是通用写法，entry 内唯一
                "link_sel": "div.entry-title h4 a",
                # 🌟 selector 命中 div.entry，不是链接节点；取 h4 内的 a
                "date_sel": "div.entry-title h4.color-food",
                # 🌟 日期是右列 h4.color-food；color-food 是日期专属颜色类名，唯一
                "min_count": 1
            }
            ,
            {
                "name": "宁夏大学信工院类图文卡片（.list-img + .list-info 复合结构）",
                "selector": ".list-img li, div.list-img li",
                "title_sel": "",  # 留空以优先提取 <a> 标签中 100% 完整的 title 属性值，规避缩略内容干扰
                "link_sel": "a",
                "date_sel": ".list-info span, span",  # 锁定 list-info 下的 span 节点，由底层清洗器自动剔除“日期：”前缀
                "min_count": 1
            },
            {
                "name": "四川大学类文字列表（ul.data-list + a.data-one + .news-time 复合结构）",
                "selector": "ul.data-list li, .data-list li",
                "title_sel": "a.data-one",
                "link_sel": "a",
                "date_sel": "span.news-time",  # 锁定 news-time 节点作为时间来源，斜杠格式由底层解析器自动还原
                "min_count": 1
            },
            {
                "name": "吉大二院采购公告列表（div.nav-list 平行结构）",
                "selector": "div.nav-list",
                # 🌟 每个公告就是一个 div.nav-list，平行排布；用容器类名直接命中条目
                "title_sel": "a[href]",
                # 🌟 标题 a 自身无专属类名，但 div.nav-list 内只有一个 a，是唯一链接
                "link_sel": "a[href]",
                # 🌟 selector 命中 div，不是链接节点；链接在 div 内的 a 上
                "date_sel": "span",
                # 🌟 日期就是裸 span，div.nav-list 内只有一个 span，不会撞到别的
                "min_count": 1
            }
            ,
            {
                "name": "南科大深港微电子学院通知公告列表（notice_new + left_date 双段日期结构）",
                "selector": "#notice_new .notice_new > ul > li",
                # 🌟 #notice_new 是整块通知区域外壳，.notice_new > ul > li 精准命中公告条目，避开底部分页 .pagination
                "title_sel": "div.right_content h3",
                # 🌟 标题在 a 内部的 right_content > h3
                "link_sel": "a[href]",
                # 🌟 selector 命中 li，不是链接节点；链接在 li 内第一个 a 上
                "date_sel": "div.left_date p",
                # 🌟 日期被拆成 big=月日、small=年份；取 p 容器后需在抓取层拼成 YYYY-MM-DD
                "min_count": 1
            }
            ,
            {
                "name": "南信工研究生院通知列表（div.list > ul > li 极简结构）",
                "selector": "div.list > ul > li",
                # 🌟 div.list 是外壳（页面上唯一带此类的容器），ul 在其内是唯一子 ul，> li 直接子选择器精准命中
                "title_sel": "a[target='_blank']",
                # 🌟 标题 a 自带 target="_blank" 属性，借此排除任何不带此属性的导航链接
                "link_sel": "a[target='_blank']",
                # 🌟 selector 命中 <li> 不是链接节点，必须显式取 a 的 href
                "date_sel": "span",
                # 🌟 日期就是裸 span，紧跟在 a 后，没有专属类名；li 内只有一个 span，不会撞到别的
                "min_count": 1
            }
            ,
            {
                "name": "华中师大前沿交叉研究院通知列表（a.news-list-item + datebox 双段日期结构）",
                "selector": "a.news-list-item",
                # 🌟 a.news-list-item 自身即条目节点；同时承担"链接节点"角色，一箭双雕
                "title_sel": "h3.news-list-title",
                # 🌟 标题是 h3，类名 news-list-title 精准唯一
                "link_sel": "",
                # 🌟 留空：selector 命中的 a 自身就是链接节点，href 在 a 上直接取
                "date_sel": "div.news-list-datebox",  # 注意：datebox 包含 day + month 两段，需在抓取层拼接
                "min_count": 1
            }
            ,
            {
                "name": "北航计算机院类图文卡片（.ej_nr + .sj + p.bt 复合结构）",
                "selector": ".ej_nr li, .list li",
                "title_sel": "p.bt",
                "link_sel": "a",
                "date_sel": ".sj",  # 锁定 sj 容器，由底层时间清洗器自动拼合 p(月-日) 与 span(年)
                "min_count": 1
            },
            {
                "name": "北科大通用类图文列表（.gpArticleDate + .gpArticleTitle 复合结构）",
                "selector": "ul[class*='block-list'] li, .block-list li, .block-list54 li",
                # 🌟 通配符模糊匹配，完美对齐 block-list / block-list54 / block-list82 等各种版本
                "title_sel": ".gpArticleTitle",  # 🌟 彻底泛化，同时兼容 a.gpArticleTitle 与 p.gpArticleTitle 两类排版
                "link_sel": "a",
                "date_sel": ".gpArticleDate",  # 锁定 gpArticleDate
                "min_count": 1
            }
            ,
            {
                "name": "GPower CMS 外国语学院本科生通知列表（page-list17 + block-list54 结构）",
                "selector": "div.page-list17 ul.block-list54 > li",
                "title_sel": "a.gpArticleTitle",
                "link_sel": "a.gpArticleTitle",
                "date_sel": "span.gpArticleDate",
                "min_count": 1,
            }

            ,
            {
                "name": "北师大高研院通知公告列表（ul.listconrl > li 经典结构）",
                "selector": "ul.listconrl > li",
                # 🌟 li 即条目节点；ul.listconrl 限定外壳，避免误抓同页其他 ul 列表
                "title_sel": "a.listconrla",
                # 🌟 标题 a 自带专属类名 listconrla，精准唯一
                "link_sel": "a.listconrla",
                # 🌟 selector 命中 <li>，不是链接节点，必须显式取 href
                "date_sel": "span.news-dates",
                "min_count": 1
            }
            ,
            {
                "name": "北师大新传院类图文卡片（.listbox + a.item 复合结构）",
                "selector": ".tzgg .listbox a.item, .listbox a.item",
                "title_sel": ".text h3",
                "link_sel": "",  # 留空表示当前 a 标签自身即为链接节点
                "date_sel": ".timebox .time",  # 锁定 timebox 内的 time 容器，由底层时间清洗器自动拼合 em(日) 与 span(年月)
                "min_count": 1
            },
            {
                "name": "北交大经管类图文列表（ul.all_list + a[href*='show-'] 复合结构）",
                "selector": "ul.all_list > li > p",
                "title_sel": "a[target='_blank']",
                "link_sel": "a[target='_blank']",
                "date_sel": "span.time_all",
                "min_count": 1
            },
            {
                "name": "内大研究生院类图文卡片（.list + a.hd + .rq 倒置拆分结构）",
                "selector": ".list a.hd, a.hd",
                "title_sel": "h3.ellipsis, h3",
                "link_sel": "",  # 留空表示当前 a 标签自身即为链接节点
                "date_sel": ".rq",  # 锁定 rq 容器，由底层的模式 C 反置重组器进行秒级还原
                "min_count": 1
            },
            {
                "name": "兰大大气院类图文卡片（main + .flex-col 复合结构）",
                "selector": "main div.flex-col a, main .flex-col a",
                # 🌟 使用 HTML5 标准唯一的 main 标签锁死正文，彻底屏蔽外部一切 header/footer 导航干扰
                "title_sel": "span.font15",
                "link_sel": "",  # 留空表示当前 a 标签自身即为链接节点
                "date_sel": "span.font13",  # 显式限制为 font13，避开前面 font15 标题的干扰
                "min_count": 1
            },
            # {    "name": "兰大大气院通知公告列表（main + div.flex-col 容器结构）",
            #      "selector": "main div.flex-col > a",    # 🌟 用 main 替换 .tab-content 作为外壳约束；显式带 flex-col 避免误抓面包屑区的“网站首页”链接
            #      "title_sel": "span.font15",
            #      "link_sel": "",          # 留空：当前 a 标签自身即为链接节点
            #      "date_sel": "span.font13",
            #      "min_count": 1
            # },
            {
                "name": "中科大网安院类图文卡片（li.cle + .text + .time 复合结构）",
                "selector": "li.cle, .view_bg li",
                "title_sel": ".text",
                "link_sel": "a",
                "date_sel": ".time",  # 锁定 time 容器，由底层时间清洗器自动剥离中括号 []
                "min_count": 1
            },
            # ==================== 新增：中国矿业大学 yclb skbt-1 分裂日期卡片 ====================
            {
                "name": "中国矿业大学 yclb skbt-1 (tm-3 + tm-4 分裂日期)",
                "selector": "div.yclb a.skbt-1, .yclb .skbt-1",
                "title_sel": ".btt-4",
                "link_sel": "",  # 当前 a 标签即为链接
                "date_sel": ".time-2",
                "min_count": 1
            },
            # ==================== 新增：博士招生 wp_news_w7 多table结构 ====================
            {
                "name": "博士招生 wp_news_w7 多table表格结构",
                "selector": "#wp_news_w7 table, #wp_news_w7 tr, div[id^='wp_news_w'] table",
                "title_sel": "td:first-child a, td a",
                "link_sel": "td:first-child a, td a",
                "date_sel": "td[width='30px'], td:last-child, div[style*='white-space:nowrap']",
                "min_count": 1
            },
            # ==================== 新增：人大统计与大数据研究院 macnulli 结构 ====================
            {
                "name": "人大统计与大数据研究院 macnulli (libiao日期)",
                "selector": "div.macnulli ul li, .macnulli li",
                "title_sel": "a",  # 先取 a 标签整体
                "link_sel": "a",
                "date_sel": "span.libiao",
                "min_count": 1
            },
            {
                "name": "人大智慧治理学院 blog-rt-box-item (onclick 跳转)",
                "selector": "li.blog-rt-box-item, .blog-rt-box-item",
                "title_sel": ".blog-rt-box-item-lf span",
                "link_sel": "",  # 留空，使用 onclick 解析
                "date_sel": ".blog-rt-box-item-rt",
                "min_count": 1
            },
            {
                "name": "人大公管类图文卡片（.listBox + a + .listTxt h6 复合结构）",
                "selector": ".listBox a, div.listBox a",
                "title_sel": ".listTxt h6",
                "link_sel": "",  # 留空表示当前 a 标签自身即为链接节点
                "date_sel": ".time",  # 锁定 time 容器，由底层时间清洗器自动拼合 b(日) 与 p(年月)
                "min_count": 1
            },
            {
                "name": "东北大学文法学院类卡片（.ao_newsLists + a.ao_each 复合结构）",
                "selector": ".ao_newsLists a.ao_each, a.ao_each",
                "title_sel": "span.hkl_paging1_right_a",
                "link_sel": "",  # 留空表示当前 a 标签自身即为链接节点
                "date_sel": "span.ao_newsdate",  # 显式限制为 ao_newsdate，避免前面 > 与后面 dot_span 等多重 span 的干扰
                "min_count": 1
            },
            {
                "name": "云师大体育院类图文卡片（.text-list2 + .date2 复合结构）",
                "selector": "ul.text-list2 li, .text-list2 li",
                "title_sel": "",  # 留空以优先提取 <a> 标签中 100% 完整的 title 属性值，拒绝被省略号截断的 h3 文本
                "link_sel": "a",
                "date_sel": ".date2",  # 锁定 date2 容器，由底层清洗器自动对齐拼装
                "min_count": 1
            },
            {
                "name": "通用 VSB 系统表格样式（table[class^='winstyle'] + .cXXXX 结构）",
                "selector": "table[class^='winstyle'] tr",
                "title_sel": "a[class^='c']",  # 🌟 彻底移除了后置的 `, a` 兜底，确保标题绝对锁死真实公告，排除分类干扰
                "link_sel": "a[class^='c']",
                "date_sel": "span[class^='timestyle']",
                "min_count": 1
            },
            {
                "name": "内蒙古农大林学艺术院图文卡片（.lists-news + .news_title 复合结构）",
                "selector": ".lists-news li, .lists-news ul li",
                "title_sel": ".news_title",
                "link_sel": "a",
                "date_sel": ".lists-news-left",
                "min_count": 1
            },
            {
                "name": "北交大交通运输院类图文列表（li.list_down + .list_down_title 复合结构）",
                "selector": "li.list_down, .list_down",
                "title_sel": ".list_down_title a",
                "link_sel": ".list_down_title a",  # 显式限制为标题内的 A 标签，完美避开后面无意义的按钮型 A 标签
                "date_sel": ".list_down_btn",  # 锁定日期容器，中括号 [] 会被底层清洗器自动完美剥离
                "min_count": 1
            },
            {
                "name": "北交大经管类图文列表（ul.all_list + a[href*='show-'] 复合结构）",
                "selector": "ul.all_list p, .all_list p",  # 🌟 将行容器锁定为 p，实现多行公告拆分
                "title_sel": "a[href*='show-']",  # 🌟 显式锁定含有 'show-' 规则的真实 A 标签，避开分类链接
                "link_sel": "a[href*='show-']",  # 🌟 同上
                "date_sel": "span.time_all",  # 🌟 定位至 time_all，点号时间格式由底层解析器自动对齐
                "min_count": 1
            },
            {
                "name": "北语国传类图文卡片（.newlist + a.title + a.date 复合结构）",
                "selector": ".newlist li, .newlist ul li",
                "title_sel": "a.title",
                "link_sel": "a.title",
                "date_sel": "a.date, .date",  # 显式指向含有 class="date" 的 A 标签，确保完美提取日期
                "min_count": 1
            },
            {
                "name": "华东交大类嵌套表格（.p_list table tr 穿透结构）",
                "selector": "td.p_list table tr, .p_list table tr",
                "title_sel": "a",
                "link_sel": "a",
                "date_sel": "div",
                "min_count": 1
            },
            {
                "name": "华东师大音乐院类图文卡片（.column-news-item 复合自关联 A 结构）",
                "selector": "a.column-news-item, .column-news-item",
                "title_sel": "span.column-news-title",  # 留空以优先提取 <a> 标签中 100% 完整的 title 属性值
                "link_sel": "a",
                "date_sel": "span.column-news-date",  # 显式限制为 .news_meta，彻底避开前置标题 span.news_title 的干扰
                "min_count": 1
            }
            ,
            {
                "name": "华东理工类图文列表（ul.news_list + span.news_meta 复合结构）",
                "selector": "ul.news_list li, .news_list li",
                "title_sel": "",  # 留空以优先提取 <a> 标签中 100% 完整的 title 属性值
                "link_sel": "a",
                "date_sel": "span.news_meta",  # 显式限制为 .news_meta，彻底避开前置标题 span.news_title 的干扰
                "min_count": 1
            },
            {
                "name": "华侨大学招生网类图文列表（ul.e2 + a.title 复合结构）",
                "selector": "ul.e2 li, .e2 li",
                "title_sel": "a.title",
                "link_sel": "a.title",  # 显式限制为 a.title，彻底避开前置的分类导航 A 标签干扰
                "date_sel": "span.info",  # 提取发布时间，由底层清洗器自动剔除“日期：”前缀及“点击：”后缀
                "min_count": 1
            },
            {
                "name": "北师大体育院文字卡片（.list_tzl + dl 拆分卡片）",
                "selector": ".list_tzl dl, #dnn_ctr579_ModuleContent dl",
                "title_sel": "dd a",
                "link_sel": "dd a",
                "date_sel": "dt",
                "min_count": 1
            },
            {
                "name": "厦大海洋院卡片布局（.notice-card + .notice-card__title 复合结构）",
                "selector": ".notice-card, div.notice-card",
                "title_sel": ".notice-card__title",
                "link_sel": "a.notice-card__link",
                "date_sel": ".notice-card__date",
                "min_count": 1
            },
            {
                "name": "天职师大类 VSB 系统表格（table[class^='winstyle'] 经典结构）",
                "selector": "table[class^='winstyle'] tr",
                "title_sel": "a[class^='c'], a",
                "link_sel": "a",
                "date_sel": "span[class^='timestyle']",
                "min_count": 1
            },
            {
                "name": "天职师大外国语学院文字卡片（.inn_com.mtsj + h2.l1 复合列表）",
                "selector": ".inn_com li, .mtsj li, li[id^='line_u15_']",
                "title_sel": "h2.l1, h2",
                "link_sel": "a",
                "date_sel": ".mtdate",
                "min_count": 1
            },
            {
                "name": "天职师大类文字卡片（.timg-linfo + .spanys 结构）",
                "selector": ".text-list li, li[id^='line_u13']",
                "title_sel": "h3",  # 提取 h3 全文本，包含的日期会被底层的除噪器自动完美剔除，保留纯净标题
                "link_sel": "a",
                "date_sel": "span.spanys, .spanys"
            },
            {
                "name": "山师大类图文卡片（.d1-list + a 倒置包裹结构）",
                "selector": "ul.d1-list a, .d1-list a",
                "title_sel": "p.tit",
                "link_sel": "",  # 留空表示当前 a 标签自身即为链接节点
                "date_sel": "p.date span, .date",
                "min_count": 1
            },
            {
                "name": "通用ny-list类文字卡片（.ny-list/.nav-list + span.date 结构）",
                "selector": ".ny-list li, .nav-list li",
                "title_sel": "",  # 留空以优先提取 <a> 标签中 100% 完整的 title 属性值
                "link_sel": "a",
                "date_sel": "span.date, span",
                "min_count": 1
            },
            {
                "name": "武汉轻工大学类图文卡片（.list-boxes + h2 a + .list-actions 结构）",
                "selector": ".list-boxes, div.list-boxes",
                "title_sel": "h2 a",
                "link_sel": "h2 a",  # 显式限制为 h2 内的 A 标签，避开“查看更多”及“点击量图标”干扰链接
                "date_sel": ".list-actions > span",  # 直属子代选择器，完美绕过 A 标签内部嵌套的空点击量计数 span
                "min_count": 1
            },
            {
                "name": "河海大学类嵌套表格（.wp_article_list_table 专属内层结构）",
                "selector": "table.wp_article_list_table table tr, .wp_article_list_table table tr",
                "title_sel": "a",
                "link_sel": "a",
                "date_sel": "td[align='right'], div",
                "min_count": 1
            },
            {
                "name": "河海大学类双层嵌套表格（#wp_news_w2 精准内层结构）",
                "selector": "#wp_news_w2 table table tr",  # 🌟 彻底移除了全局通用匹配，确保仅扫描公告正文区
                "title_sel": "a",
                "link_sel": "a",
                "date_sel": "div",
                "min_count": 1
            },
            {
                "name": "郑州航院类极简列表（li.list1 + span 紧凑卡片）",
                "selector": "li.list1, #newcontent li",
                "title_sel": "a",
                "link_sel": "a",
                "date_sel": "span",
                "min_count": 1
            },
            {
                "name": "河南工大类图文列表（ul.ul-pagenews + .date 拆分卡片）",
                "selector": "ul.ul-pagenews li, .ul-pagenews li",
                "title_sel": "",  # 留空以优先提取 <a> 标签中 100% 完整的 title 属性值
                "link_sel": "a",
                "date_sel": ".date"
            },
            {
                "name": "内蒙古科大类文字卡片（.list_item + .title + a.more 结构）",
                "selector": "div.list_item, .list_item",
                "title_sel": ".title",
                "link_sel": "a.more",
                "date_sel": ".date",
                "min_count": 1
            },
            # 1. 浙江海大国家海洋中心图文列表（高精度，支持尾页，min_count=1）
            {
                "name": "浙江海大国家海洋中心图文列表（.news_list_word_items 复合卡片）",
                "selector": ".news_list_word_items .item, .news_list_word_items div.item",
                "title_sel": "a.item_title",
                "link_sel": "a",
                "date_sel": ".item_time",
                "min_count": 1
            },
            # 2. 清华高精尖院图文卡片（高精度，支持尾页，min_count=1）
            {
                "name": "清华高精尖院图文卡片（.n_list + .time 拆分卡片结构）",
                "selector": ".n_list li, .n_list ul li",
                "title_sel": "",
                "link_sel": "a",
                "date_sel": ".time",
                "min_count": 1
            },
            # 3. 清华医院管理研究院文字卡片（高精度，min_count=1）
            {
                "name": "清华医院管理研究院文字卡片（ul.list + line_u15 结构）",
                "selector": "ul.list li[id^='line_u'], li[id^='line_u15_']",
                "title_sel": "h3",
                "link_sel": "a",
                "date_sel": "span",
                "min_count": 1
            },
            # 4. 清华经管院学术讲座（高精度，min_count=1）
            {
                "name": "清华经管院学术讲座（.subjuct_content 复合列表）",
                "selector": ".subjuct_content, div.subjuct_content",
                "title_sel": ".subjuct_txt a",
                "link_sel": "a",
                "date_sel": ".subjuct_time",
                "min_count": 1
            },
            # 5. 温州医大国际教育学院文字卡片（高精度，min_count=1）
            {
                "name": "温州医大国际教育学院文字卡片（.ej_list2 + p.bt + p.sj 结构）",
                "selector": ".ej_list2 li, .ej_list2 ul li",
                "title_sel": "p.bt",
                "link_sel": "a",
                "date_sel": "p.sj",
                "min_count": 1
            },
            # 6. 温州医大类文字卡片（高精度，min_count=1）
            {
                "name": "温州医大类文字卡片（.other_list + p + em 直连结构）",
                "selector": ".other_list a, div.other_list a",
                "title_sel": "p",
                "link_sel": "",
                "date_sel": "em",
                "min_count": 1
            },
            # 7. 福建中医药大学类文字卡片（高精度，min_count=1）
            {
                "name": "福建中医药大学类文字卡片（.newslist 紧凑直连 A 结构）",
                "selector": ".newslist a, div.newslist a",
                "title_sel": "",
                "link_sel": "",
                "date_sel": "span",
                "min_count": 1
            },
            # 8. 西南财经大学类图文卡片（高精度，min_count=1）
            {
                "name": "西南财经大学类图文卡片（a.list_item + h1 复合关联结构）",
                "selector": "a.list_item, .list_item",
                "title_sel": "h1",
                "link_sel": "",
                "date_sel": ".date",
                "min_count": 1
            },
            # 9. 清华大学研招网类图文列表（高精度，min_count=1）
            {
                "name": "清华大学研招网类图文列表（.admission 复合自关联卡片）",
                "selector": ".admission li a, .admission a",
                "title_sel": "",
                "link_sel": "",
                "date_sel": ".time",
                "min_count": 1
            },
            # 10. 图文卡片自关联布局（重庆大学等，高精度，min_count=1）
            {
                "name": "图文卡片自关联布局（如重庆大学等 a.subListOne-item 复合结构）",
                "selector": ".subListOne a.subListOne-item, a.subListOne-item",
                "title_sel": ".p1",
                "link_sel": "",
                "date_sel": ".time",
                "min_count": 1
            },
            # --------------------------- 以下为兜底防线 ---------------------------
            # 11. 经典卡片布局（如 .news-item 等，模糊兜底，min_count=3 防抢占）
            {
                "name": "经典卡片布局（如.item, .news-item）",
                "selector": ".item, div.item, .newstitem, .column-news-item, .article-item, .subpage-list-item, .notice-item, a.list-item, .ej_list2 li, .subNoticeList li, .news-item",
                "title_sel": "h2, p.title, .column-news-title, .article-item-title, p, h3, .ni-title",
                "link_sel": "a",
                "date_sel": ".date, .column-news-date, .article-item-info, span, .ni-date, p.sj",
                "min_count": 3
            },
            # 12. line_u 标识类通用列表（模糊兜底，min_count=3 防抢占）
            {
                "name": "line_u标识类布局（如陕师大、长安大学、吉大等）",
                "selector": "li[id^='line_u'], dl[id^='line_u'], tr[id^='line_u'], li.wow",
                "title_sel": ".ltrlb_tit, h3, h2, a, .titline",
                "link_sel": "a",
                "date_sel": ".time1, .time, span, time",
                "min_count": 3
            },
            # 13. 传统表格及 Joomla 行布局（模糊兜底，min_count=3 防抢占）
            {
                "name": "传统表格及Joomla行布局（如各类嵌套列表、Joomla内容行）",
                "selector": "tr[class*='cat-list-row'], table.wp_article_list_table tr, table tr:has(a)",
                "title_sel": ".list-title a, a",
                "link_sel": "a",
                "date_sel": ".list-date, td[align='right'], td",
                "min_count": 3
            },
            # 14. 裸标签兜底布局（极度宽泛，限制最高，min_count=3 防抢占）
            {
                "name": "通用裸列表项（如.content-list或裸UL/LI结构）",
                "selector": ".content-list li, .content-list lu li, ul li, UL li",
                "title_sel": "a",
                "link_sel": "a",
                "date_sel": "span",
                "min_count": 3
            }
        ]

        for cfg in configs:
            nodes = soup.select(cfg["selector"])
            if not nodes:
                continue

            current = []
            valid_date_count = 0
            min_count = cfg.get("min_count", 1)

            bypass_density_check = (not cfg.get("date_sel"))

            for node in nodes:
                if cfg.get("link_sel"):
                    a = node.select_one(cfg["link_sel"])
                else:
                    a = node if node.name == 'a' else node.find('a', href=True)

                if not a or not a.get('href'):
                    continue

                # 核心防线：过滤无意义 JS 占位符、空链接以及【栏目索引页/导航主页】（彻底剥离分类菜单）
                href_raw = a.get('href', '').strip()
                href_lower = href_raw.lower()
                if any(p in href_lower for p in ['javascript:', '#', 'mailto:']) or \
                        any(href_lower.endswith(p) for p in
                            ['index.htm', 'index.html', 'main.htm', 'list.htm', 'list.html', 'list_1.htm']) or \
                        re.search(r'/(?:tzgg|zhxw|sqyr|rcpy|bkspy|yjspy|xsgz|xszg)\d*(?:\.htm|\.html)?$', href_lower):
                    continue

                # 🌟 必须且只能在这里，先对绝对 URL 变量进行拼接与初始化！
                url = urljoin(base_url, href_raw)

                title_node = node.select_one(cfg["title_sel"]) if cfg["title_sel"] else None
                title = title_node.get_text(strip=True) if title_node else a.get('title', '').strip() or a.get_text(
                    strip=True)
                title = re.sub(r'[…\s\.]+$', '', title).strip()

                if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                    continue

                # =========================================================================
                # 🌟 第一优先级：尝试直接从物理 URL 中高速提取时间（100% 准确、免除一切 DOM 噪音）
                # =========================================================================
                date_text = self._extract_date_from_url(url)

                # =========================================================================
                # 🌟 第二优先级：若 URL 中无时间，退化至 DOM 拆分或特定容器选择器匹配
                # =========================================================================
                if not date_text:
                    date_text = self._extract_split_date_from_container(node, title)
                    if not date_text and cfg["date_sel"]:
                        date_node = node.select_one(cfg["date_sel"])
                        if date_node:
                            # 递归解析其内部是否含有更复杂的拆分标签
                            date_text = self._extract_date_from_element(date_node)
                            if not date_text:
                                date_text = date_node.get_text(strip=True)
                        else:
                            date_text = self._extract_date_from_element(node)

                # 累加有效时间数
                if date_text and len(date_text.strip()) > 3:
                    valid_date_count += 1

                # 智能除噪（防止 A 标签内建 span 日期污染标题）
                if date_text:
                    title = title.replace(date_text, "").strip()
                    title = title.replace(date_text.replace('.', '-'), "").strip()
                    title = title.replace(date_text.replace('-', '.'), "").strip()
                    title = title.strip('- \t\n\r|｜[]()（）')

                current.append({"title": title, "url": url, "date": date_text})

            # 规则分级防御判定
            # 🌟 规则分级防御判定（引入 bypass_density_check 免检锁，支持川大等无时间页面安全通关）
            if len(current) >= min_count and (
                    bypass_density_check or len(current) == 0 or (valid_date_count / len(current) >= 0.6)):
                self._recalc_current_dates(current)
                self.log.emit(
                    f"[自动识别 | 布局匹配] 命中 '{cfg['name']}' 经典规则 -> 共提取 {len(current)} 条数据")
                return current

            # 特异性：部分基于自定义点击行为非标准 A 链接的解析
            onclick_items = []
            for li in soup.find_all(['li', 'div', 'tr']):
                onclick = li.get('onclick', '') or li.get('onClick', '') or ""
                a = li.find('a')
                a_onclick = (a.get('onclick', '') or a.get('onClick', '')) if a else ""

                combined_click = str(onclick) + str(a_onclick)
                if combined_click and any(keyword in combined_click for keyword in
                                          ['checkXnwk', 'window.location', 'window.open', 'location=']):
                    onclick_items.append((li, a, combined_click))

            if onclick_items:
                current = []
                for li, a, click_str in onclick_items:
                    title = a.get('title', '').strip() if a else ""
                    if not title and a:
                        title = a.get_text(strip=True)
                    if not title:
                        title = li.get_text(strip=True)
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4:
                        continue

                    url = ""
                    match_fn = re.search(r"(?:window\.location\.href|window\.location|location)\s*=\s*['\"](.*?)['\"]",
                                         click_str)
                    if match_fn:
                        url = urljoin(base_url, match_fn.group(1).strip())
                    elif "window.location.href" in click_str:
                        match_href = re.search(r"window\.location\.href\s*=\s*['\"](.*?)['\"]", click_str)
                        if match_href:
                            url = urljoin(base_url, match_href.group(1).strip())

                    if not url:
                        continue

                    date_text = self._extract_date_from_element(li)
                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 1:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[自动识别 | 事件解析] 破译绑定在 onclick 上的动态行为，提取 {len(current)} 条")
                    return current

            return []

    # ====================== 智能微观分裂日期重组器 ======================
    def _extract_split_date_from_container(self, container, title):
        """
        微观 DOM 邻近分裂日期重组器
        """
        if not container:
            return None

        text_nodes = []
        for element in container.find_all(text=True):
            text_str = element.strip()
            if not text_str:
                continue
            if title and (text_str in title or title in text_str):
                continue
            text_str = re.sub(r'[\s\xa0\u3000\r\n|｜\[\]\(\)（）>›»▪•*◎：:：\-]+', ' ', text_str).strip()
            if text_str:
                text_nodes.append(text_str)

        combined_text = " ".join(text_nodes)

        # 1. 尝试直接进行标准 YYYY-MM-DD、YYYY/MM/DD、YYYY.MM.DD 捕获
        m_standard = re.search(r'\b(20\d{2})\s*[-/年.]\s*([01]?\d)\s*[-/月.]\s*([0-3]?\d)\s*日?\b', combined_text)
        if m_standard:
            return f"{m_standard.group(1)}-{int(m_standard.group(2)):02d}-{int(m_standard.group(3)):02d}"

        # 2. 局部拆分检测 A: [年-月] (如2024-06) + [日] (如03)
        year_month, day = None, None
        for node in text_nodes:
            m_ym = re.search(r'\b(20\d{2})[-/.](\d{1,2})\b', node)
            if m_ym:
                year_month = f"{m_ym.group(1)}-{int(m_ym.group(2)):02d}"
                break
        for node in text_nodes:
            if year_month and (node in year_month or any(p in node for p in year_month.split('-'))):
                continue
            m_day = re.search(r'\b([0-3]?\d)\b', node)
            if m_day:
                val = int(m_day.group(1))
                if 1 <= val <= 31:
                    day = f"{val:02d}"
                    break
        if year_month and day:
            return f"{year_month}-{day}"

        # 3. 局部拆分检测 B: [年] (如2024) + [月-日] (如06-25)
        year_only, month_day = None, None
        for node in text_nodes:
            m_y = re.search(r'\b(20\d{2})\b', node)
            if m_y:
                year_only = m_y.group(1)
                break
        for node in text_nodes:
            if year_only and year_only in node:
                continue
            m_md = re.search(r'\b([01]?\d)[-/.]?([0-3]?\d)\b', node)
            if m_md:
                val_m = int(m_md.group(1))
                val_d = int(m_md.group(2))
                if 1 <= val_m <= 12 and 1 <= val_d <= 31:
                    month_day = f"{val_m:02d}-{val_d:02d}"
                    break
        if year_only and month_day:
            return f"{year_only}-{month_day}"

        return None

    # ====================== 纯链接兜底分支 ======================
    def _extract_via_raw_links_fallback(self, html, base_url):
        soup = BeautifulSoup(html, 'html.parser')
        candidate_items = []

        for a in soup.find_all('a', href=True):
            title = a.get_text(strip=True)
            href = a.get('href', '').strip()
            if len(title) < 8 or href.startswith(('javascript:', '#')):
                continue
            if any(k in title for k in ['首页', '上页', '下页', '尾页', '登录', '更多>>', '查看更多']):
                continue
            full_url = urljoin(base_url, href)
            date_text = self._extract_date_from_element(a)
            candidate_items.append({"title": title, "url": full_url, "date": date_text})

        seen = set()
        items = []
        for item in candidate_items:
            if item['title'] not in seen:
                seen.add(item['title'])
                items.append(item)

        if len(items) >= 3:
            self.log.emit(f"[自动识别 | 兜底] 未找到已知样式排版，切换链接广度搜索，提取 {len(items)} 条")
            return items[:80]

        self.log.emit(f"[自动识别 | 警告] 该网页排版结构特殊，仅提取到 {len(items)} 条链接")
        return items

    # ====================== 日期清洗与解析逻辑 ======================
    def _extract_date_from_element(self, element):
        """通用高容错日期文本提取（优化重构版：解决自锁、自持判定与两位数年份自动补全）"""
        if not element:
            return ""

        # 🌟 过滤明显的阅读数/点击量自身标签，防止直接提取
        if element.name:
            el_class = " ".join(element.get('class', [])).lower() if element.get('class') else ""
            el_id = str(element.get('id', '')).lower()
            if any(k in el_class or k in el_id for k in ['visit', 'click', 'view', 'count', 'hit']):
                return ""

        # 1. 尝试直接匹配标准 YYYY-MM-DD
        html_str = str(element)
        match_std = re.search(r'\b(\d{4}-\d{1,2}-\d{1,2})\b', html_str)
        if match_std:
            return match_std.group(1)

        # =========================================================================
        # 🌟 核心优化 1：自持容器智能判定（解决传入节点本身就是时间容器时，向下 select 失败的限制）
        # =========================================================================
        time_div = None
        classes = element.get('class', []) if element.name else []
        containers = ['time', 'posttime', 'date', 'date2', 'rq', 'sj', 'mtdate', 'notice-card__date', 'lists-news-left']
        if element.name in ['time'] or any(c in classes for c in containers):
            time_div = element
        else:
            time_div = element.select_one(
                '.time, .posttime, .date, .date2, .rq, .sj, .mtdate, .notice-card__date, .lists-news-left')

        if time_div:
            # 🌟 核心优化 2：剔除 naked 'p' 和 'span' 标签，引入高精度的 '.md' 标签，彻底避免 day 与 year 发生同标签覆盖冲突（自锁）
            day = time_div.select_one('.day, .md, .date-day, strong, b, em, h3, h4, .notice-card__day, .number, .d2')
            year = time_div.select_one(
                '.year, .year-month, .month, .date-y-m, h6, .notice-card__month, .news_time, .d1 ,span')

            # 🌟 核心：只有当 day 与 year 分别指向两个不同的子节点时才进行分拆拼合，防止逻辑自锁
            if day and year and day != year:
                day_text = day.get_text(strip=True)
                year_text = year.get_text(strip=True).replace('/', '-').replace('.', '-').strip()

                # 1. 核心补全：如果是纯 2 位数字年份（如单独的 "26"），补全为 "2026"
                if year_text.isdigit() and len(year_text) == 2:
                    year_text = f"20{year_text}"

                # 2. 🌟 新增：如果是 2 位年份带月份（如 "26-06" 或 "26.06"），智能转换为 "2026-06"
                m_short_ym = re.match(r'^(\d{2})[-.](\d{1,2})$', year_text)
                if m_short_ym:
                    short_year = int(m_short_ym.group(1))
                    month = m_short_ym.group(2)
                    # 动态估算世纪年份：70以上归为19xx，70以下归为20xx
                    full_year = 2000 + short_year if short_year < 70 else 1900 + short_year
                    year_text = f"{full_year}-{month.zfill(2)}"

                # 模式 A：标准的 [年-月] + [日] (如 year="2026-06", day="23")
                if re.match(r'^\d{4}[-.]\d{1,2}$', year_text) and day_text.isdigit():
                    return f"{year_text}-{day_text.zfill(2)}"

                # 模式 B：河南工大/首师大等倒置的 [月.日] + [年] (如 day_text="12.15", year_text="2025")
                if '.' in day_text and year_text.isdigit() and len(year_text) == 4:
                    return f"{year_text}-{day_text.replace('.', '-')}"

                # 模式 C：内大等极致倒置的 [月] + [年-日]（如 day_text="12"(月), year_text="2021-16"(年-日)）
                m_rq = re.match(r'^(\d{4})-(\d{1,2})$', year_text)
                if m_rq and day_text.isdigit():
                    year_val = m_rq.group(1)
                    day_val = m_rq.group(2).zfill(2)
                    month_val = day_text.zfill(2)
                    return f"{year_val}-{month_val}-{day_val}"

                # 模式 D：北航/浙大等倒置的 [月-日] + [年]（如 day_text="04-13"(月-日), year_text="2026"(年)）
                if '-' in day_text and year_text.isdigit() and len(year_text) == 4:
                    return f"{year_text}-{day_text}"

        # 2. 精准寻找代表发布时间的样式元素
        selectors = [
            '.date', '.time', 'span.date', 'span.time', '.PostTime', 'time',
            '.article-date', '.news-info', 'small', 'em', '.list-date', '.ni-date'
        ]
        for sel in selectors:
            node = element.select_one(sel)
            if node:
                text = node.get_text(strip=True)
                if re.search(r'\d', text):
                    return text

        # 3. 标签级别深度扫描
        for tag in ['span', 'div', 'p', 'li', 'td', 'time', 'small']:
            for item in element.find_all(tag):
                text = item.get_text(strip=True)
                m = re.search(r'(\d{4}-\d{1,2}-\d{1,2})', text)
                if m:
                    return m.group(1)

        # 4. 消除标题后利用正则在残留文本中寻找时间
        text = element.get_text(" ", strip=True)

        # =========================================================================
        # 🌟 核心升级：增加对 "DD YYYY.MM" / "DD YYYY-MM" (如 "08 2023.12") 反置文本的直接提取
        # =========================================================================
        m_reverse = re.search(r'\b(\d{1,2})\s+(\d{4})[-./](\d{1,2})\b', text)
        if m_reverse:
            day = m_reverse.group(1).zfill(2)
            year = m_reverse.group(2)
            month = m_reverse.group(3).zfill(2)
            return f"{year}-{month}-{day}"  # 直接组装并返回标准 YYYY-MM-DD
        # =========================================================================

        # =========================================================================
        # 🌟 核心升级 2：增加对 "DD/YYYY-MM" (如 "02/2026-07") 反置斜杠文本的直接提取
        # =========================================================================
        m_slash_reverse = re.search(r'\b(\d{1,2})\s*/\s*(\d{4})[-./](\d{1,2})\b', text)
        if m_slash_reverse:
            day = m_slash_reverse.group(1).zfill(2)
            year = m_slash_reverse.group(2)
            month = m_slash_reverse.group(3).zfill(2)
            return f"{year}-{month}-{day}"
        # =========================================================================

        patterns = [
            r'(\d{4}[-/年.]\d{1,2}[-/月.]\d{1,2})',
            r'(\d{4}[-/]\d{1,2}[-/]\d{1,2})',
            r'(\d{4}年\d{1,2}月\d{1,2}日?)',
            r'(\d{1,2}月\d{1,2}日?)'
        ]
        for pat in patterns:
            m = re.search(pat, text)
            if m:
                return m.group(1)

        return ""

    def _parse_date(self, date_text):
        """
        全能型时间解析器（超级对齐自愈版：支持 YYYY-MMDD 物理粘连与二位数短年份还原）
        """
        if not date_text:
            return "", 999, False

        original = str(date_text).strip()
        now = datetime.now()

        # =========================================================================
        # 🌟 新增防御：强效拦截纯数字点击量、阅读数干扰（如 10, 843, 3693 等无意义整型）
        # =========================================================================
        if original.isdigit():
            # 纯数字只允许是 6 位(YYMMDD)或 8 位(YYYYMMDD)
            if len(original) not in [6, 8]:
                return "", 999, False
            # 如果是 8 位，必须以合理的年份开头（19xx 或 20xx 或 21xx）
            if len(original) == 8 and not original.startswith(('19', '20', '21')):
                return "", 999, False

        # =========================================================================
        # 🌟 核心拦截盾：针对 "02/2026-07" (DD/YYYY-MM) 极速无损解析，彻底解决月份颠倒
        # =========================================================================
        m_lnnu_style = re.search(r'(\d{1,2})\s*/\s*(\d{4})[-_./\s](\d{1,2})', original)
        if m_lnnu_style:
            try:
                day_val = int(m_lnnu_style.group(1))
                year_val = int(m_lnnu_style.group(2))
                month_val = int(m_lnnu_style.group(3))

                if 1 <= month_val <= 12 and 1 <= day_val <= 31 and 1990 <= year_val <= 2050:
                    dt = datetime(year_val, month_val, day_val)
                    days_old = max(0, (now - dt).days)
                    formatted_res = dt.strftime("%Y-%m-%d")
                    # 🌟 打印控制台调试日志，方便我们一眼看清解析过程
                    print(f"【调试 - LNNU拦截盾工作】原始输入: '{original}' -> 解析成功: '{formatted_res}'")
                    return formatted_res, days_old, True
            except Exception as ex:
                print(f"【调试 - LNNU拦截盾异常】: {ex}")
                pass
        # =========================================================================

        # =========================================================================
        # 🌟 步骤 B：反向拆分文本解析（直接信任提取出的 4 位年份，不进行扣减）
        # =========================================================================
        m_rev_text = re.search(r'\b(\d{1,2})\s+(\d{4})[-_./\s](\d{1,2})\b', original)
        if m_rev_text:
            try:
                day = m_rev_text.group(1).zfill(2)
                year = m_rev_text.group(2)
                month = m_rev_text.group(3).zfill(2)
                if 1990 <= int(year) <= 2050:
                    dt = datetime(int(year), int(month), int(day))
                    days_old = max(0, (now - dt).days)
                    return dt.strftime("%Y-%m-%d"), days_old, True
            except:
                pass

        # =========================================================================
        # 🌟 步骤 B-2：反置斜杠拆分文本解析（针对 "02/2026-07"，直接信任，不进行扣减）
        # =========================================================================
        m_slash_rev = re.search(r'\b(\d{1,2})\s*/\s*(\d{4})[-_./\s](\d{1,2})\b', original)
        if m_slash_rev:
            try:
                day = m_slash_rev.group(1).zfill(2)
                year = m_slash_rev.group(2)
                month = m_slash_rev.group(3).zfill(2)
                if 1990 <= int(year) <= 2050:
                    dt = datetime(int(year), int(month), int(day))
                    days_old = max(0, (now - dt).days)
                    return dt.strftime("%Y-%m-%d"), days_old, True
            except:
                pass

        # =========================================================================
        # 🌟 核心拦截：连读产生 "2026-0615" (YYYY-MMDD) 畸形文本（直接信任年份）
        # =========================================================================
        m_merged_e = re.search(r'\b(\d{4})-(\d{2})(\d{2})\b', original)
        if m_merged_e:
            try:
                year = m_merged_e.group(1)
                month = m_merged_e.group(2).zfill(2)
                day = m_merged_e.group(3).zfill(2)
                dt = datetime(int(year), int(month), int(day))
                days_old = max(0, (now - dt).days)
                self.log.emit(
                    f"[日期解析 | 逆向还原] 成功还原 YYYY-MMDD 连读文本 {original} -> {dt.strftime('%Y-%m-%d')} ({days_old}天前)")
                return dt.strftime("%Y-%m-%d"), days_old, True
            except:
                pass

        # =========================================================================
        # 🌟 核心解析：连读产生 "122020-31" (MMYYYY-DD) 畸形文本（直接信任年份）
        # =========================================================================
        m_merged_d = re.search(r'\b(\d{2})(\d{4})-(\d{1,2})\b', original)
        if m_merged_d:
            try:
                month = m_merged_d.group(1).zfill(2)
                year = m_merged_d.group(2)
                day = m_merged_d.group(3).zfill(2)
                if not (1990 <= int(year) <= 2050):
                    raise ValueError("年份不符合现实")
                dt = datetime(int(year), int(month), int(day))
                days_old = max(0, (now - dt).days)
                self.log.emit(
                    f"[日期解析 | 逆向还原] 成功还原连读粘连文本 {original} -> {dt.strftime('%Y-%m-%d')} ({days_old}天前)")
                return dt.strftime("%Y-%m-%d"), days_old, True
            except:
                pass

        # =========================================================================
        # 🌟 核心解析：针对 "10-09.2021" (MM-DD.YYYY) 后置年份（直接信任年份）
        # =========================================================================
        m_dot_year = re.search(r'\b(\d{1,2})[-/](\d{1,2})[-/.]\s*(\d{4})\b', original)
        if m_dot_year:
            try:
                month = m_dot_year.group(1).zfill(2)
                day = m_dot_year.group(2).zfill(2)
                year = m_dot_year.group(3)
                if not (1990 <= int(year) <= 2050):
                    raise ValueError("年份不符合现实")
                dt = datetime(int(year), int(month), int(day))
                days_old = max(0, (now - dt).days)
                return dt.strftime("%Y-%m-%d"), days_old, True
            except:
                pass

        # =========================================================================
        # 🌟 核心解析：针对 "22/ 2026-05" (DD/ YYYY-MM) 拆分格式（直接信任年份）
        # =========================================================================
        m_slash_split = re.search(r'\b(\d{1,2})\s*/\s*(\d{4})[-_./\s](\d{1,2})\b', original)
        if m_slash_split:
            try:
                day = m_slash_split.group(1).zfill(2)
                year = m_slash_split.group(2)
                month = m_slash_split.group(3).zfill(2)
                if not (1990 <= int(year) <= 2050):
                    raise ValueError("年份不符合现实")
                dt = datetime(int(year), int(month), int(day))
                days_old = max(0, (now - dt).days)
                return dt.strftime("%Y-%m-%d"), days_old, True
            except:
                pass

        # =========================================================================
        # 🌟 核心解析：解析 2 位数年份的短日期 (如 "26-04-13" 或 "26/04/13")
        # =========================================================================
        m_short_year = re.search(r'\b(\d{2})[-/.](\d{1,2})[-/.](\d{1,2})\b', original)
        if m_short_year:
            try:
                year = int(m_short_year.group(1))
                month = int(m_short_year.group(2))
                day = int(m_short_year.group(3))
                if 0 <= year < 100:
                    year_full = 1900 + year if year >= 70 else 2000 + year
                    if not (1990 <= year_full <= 2050):
                        raise ValueError("年份不符合现实")
                    dt = datetime(year_full, month, day)
                    days_old = max(0, (now - dt).days)
                    return dt.strftime("%Y-%m-%d"), days_old, True
            except:
                pass

        # =========================================================================
        # 🌟 核心解析：拦截并转换非标中文月份格式（如 "九月 27, 2017"）
        # =========================================================================
        m_cn_month = re.search(r'([一二三四五六七八九十]+)月\s*(\d{1,2})[,\s]+(\d{4})', original)
        if m_cn_month:
            try:
                cn_month_map = {
                    "一": "01", "二": "02", "三": "03", "四": "04", "五": "05", "六": "06",
                    "七": "07", "八": "08", "九": "09", "十": "10", "十一": "11", "十二": "12"
                }
                month_str = cn_month_map.get(m_cn_month.group(1))
                if month_str:
                    day_str = m_cn_month.group(2).zfill(2)
                    year_str = m_cn_month.group(3)
                    formatted = f"{year_str}-{month_str}-{day_str}"
                    dt = datetime.strptime(formatted, "%Y-%m-%d")
                    days_old = max(0, (now - dt).days)
                    return formatted, days_old, True
            except:
                pass

        # =========================================================================
        # 🌟 核心解析：处理非标末尾年份黏连格式（如 "05-212024"）
        # =========================================================================
        m_merged = re.search(r'\b(\d{1,2})[-/.](\d{1,2})(\d{4})\b', original)
        if m_merged:
            try:
                month = m_merged.group(1)
                day = m_merged.group(2)
                year = m_merged.group(3)
                if not (1990 <= int(year) <= 2050):
                    raise ValueError("年份不符合现实")
                dt = datetime(int(year), int(month), int(day))
                days_old = max(0, (now - dt).days)
                return dt.strftime("%Y-%m-%d"), days_old, True
            except:
                pass

        # 4. 基础格式格式化清洗
        cleaned = re.sub(r'[年月]', '-', original)
        cleaned = re.sub(r'[日]', ' ', cleaned)
        cleaned = re.sub(r'[-/.]+', '-', cleaned)
        cleaned = re.sub(r'[\[\]\(\)（）\xa0\u3000]+', ' ', cleaned)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip('- \t\n\r')

        # 5. 使用 dateutil 工具智能解析（终极安全偏正兜底盾）
        try:
            dt = parser.parse(cleaned, fuzzy=True, yearfirst=True)

            # 🌟 新增：检查原始文本中是否本身就含有 19xx, 20xx, 21xx 这样的 4 位数年份
            has_4digit_year = bool(re.search(r'\b(19|20|21)\d{2}\b', original))

            # 🌟 只有在原始文本没有年份（属于猜测的未来时间）时，才允许自动减一年
            if not has_4digit_year and (dt - now).days > 2:
                try:
                    dt = dt.replace(year=dt.year - 1)
                except ValueError:  # 应对闰年 2 月 29 日
                    dt = dt.replace(year=dt.year - 1, day=28)
            days_old = max(0, (now - dt).days)
            return dt.strftime("%Y-%m-%d"), days_old, True
        except:
            pass

        # 6. 正则兜底解析
        patterns = [
            r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})',
            r'(\d{4})(\d{2})(\d{2})'
        ]
        for pat in patterns:
            m = re.search(pat, cleaned)
            if m:
                try:
                    year = int(m.group(1))
                    dt = datetime(year + 2000 if year < 100 else year, int(m.group(2)), int(m.group(3)))
                    days_old = max(0, (now - dt).days)
                    return dt.strftime("%Y-%m-%d"), days_old, True
                except:
                    continue

        # 7. 无年份短日期解析（如 05-10）
        m_short = re.search(r'(\d{1,2})[-/](\d{1,2})', cleaned)
        if m_short:
            try:
                dt = datetime(now.year, int(m_short.group(1)), int(m_short.group(2)))
                if (dt - now).days > 2:
                    try:
                        dt = dt.replace(year=now.year - 1)
                    except ValueError:
                        dt = dt.replace(year=now.year - 1, day=28)
                days_old = max(0, (now - dt).days)
                return dt.strftime("%Y-%m-%d"), days_old, True
            except:
                pass

        return original, 999, False

    # ====================== 寻址与探测辅助 ======================
    def _find_link_node(self, soup, base_url, link):
        """通过绝对或相对路径回寻 A 标签"""
        target_abs_url = urljoin(base_url, link) if link else ''
        if not target_abs_url:
            return None

        for possible_a in soup.find_all('a', href=True):
            href = possible_a.get('href', '').strip()
            if href.startswith(('javascript:', '#')):
                continue
            if urljoin(base_url, href) == target_abs_url:
                return possible_a
        return None

    def _find_single_row_container(self, a_tag, base_url):
        """向上爬升，寻找安全的单行局部闭包容器"""
        curr = a_tag
        parent_container = None
        for _ in range(5):
            if curr is None or curr.name == 'body':
                break

            sibling_links = []
            for a in curr.find_all('a', href=True):
                h = a.get('href', '').strip()
                if h and not h.startswith(('javascript:', '#', 'mailto:')) and 'list' not in h.lower():
                    sibling_links.append(urljoin(base_url, h))
            if len(set(sibling_links)) > 1:
                break

            curr_class = ''.join(curr.get('class', [])).lower()
            curr_id = curr.get('id', '').lower()
            if curr.name in ['li', 'tr', 'dd'] or \
                    any(x in curr_class for x in ['item', 'line', 'row', 'columns', 'col-']) or \
                    any(x in curr_id for x in ['item', 'line', 'row', 'columns']):
                parent_container = curr
                break
            parent_container = curr
            curr = curr.parent

        return parent_container if parent_container else a_tag.parent

    def _probe_detail_page(self, url):
        """向详情页发送探测，使用 htmldate 或 Gerapy 进行智能日期溯源"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            resp = requests.get(url, headers=headers, timeout=6, verify=False)
            resp.encoding = resp.apparent_encoding or 'utf-8'
            detail_html = resp.text
            if not detail_html:
                return None

            try:
                from gerapy_auto_extractor import extract_datetime
                raw_detail_date = extract_datetime(detail_html)
                if raw_detail_date:
                    if isinstance(raw_detail_date, datetime):
                        return raw_detail_date.strftime('%Y-%m-%d %H:%M:%S')
                    return str(raw_detail_date)
            except:
                pass

            try:
                from htmldate import find_date
                detail_date = find_date(detail_html)
                if detail_date:
                    return detail_date
            except ImportError:
                pass
        except Exception as e:
            self.log.emit(f"[请求 | 警告] 无法向详情页派发探测请求 -> URL: {url} | 原因: {e}")
        return None

    def _check_notice(self, notice):
        """关键词及时间双向过滤"""
        if self.days > 0:
            if notice.days_old > self.days and notice.days_old < 900:
                self.log.emit(
                    f"[过滤 | 忽略] 不满足发布时效 -> 标题: '{notice.title[:15]}...' | 相差: {notice.days_old} 天")
                return False

        if self.include_keywords:
            matched_keywords = [k for k in self.include_keywords if k in notice.title]
            if not matched_keywords:
                self.log.emit(f"[过滤 | 忽略] 标题未包含设定的关键词群 -> 标题: '{notice.title[:15]}...'")
                return False
            notice.keywords = ", ".join(matched_keywords)
        else:
            notice.keywords = "全部"

        for k in self.exclude_keywords:
            if k in notice.title:
                self.log.emit(f"[过滤 | 忽略] 标题命中了排除词 '{k}' -> 标题: '{notice.title[:15]}...'")
                return False

        return True

    def stop(self):
        self._running = False


class ScriptWorker(QThread):
    """自定义脚本执行线程"""

    progress = pyqtSignal(int, int)
    log = pyqtSignal(str)
    result = pyqtSignal(dict)
    finished = pyqtSignal()

    def __init__(self, script_paths):
        super().__init__()
        self.script_paths = script_paths
        self._running = True

    def run(self):
        total = len(self.script_paths)
        for i, script_path in enumerate(self.script_paths):
            if not self._running:
                break

            self.progress.emit(i + 1, total)
            self.log.emit(f"[脚本] 开始执行: {script_path}")

            try:
                self._execute_script(script_path)
            except Exception as e:
                self.log.emit(f"[错误] 脚本执行失败: {script_path} - {str(e)}")

        self.finished.emit()

    def _execute_script(self, script_path):
        import os
        import sys

        collected_results = []

        def emit_result(data):
            if isinstance(data, dict):
                collected_results.append(data)
                self.result.emit(data)
            else:
                self.log.emit(f"[警告] 脚本返回了无效数据格式: {type(data)}")

        script_globals = {
            '__name__': '__main__',
            '__file__': script_path,
            'emit_result': emit_result,
            'log': lambda msg: self.log.emit(f"[脚本日志] {msg}"),
        }

        script_dir = os.path.dirname(os.path.abspath(script_path))
        if script_dir not in sys.path:
            sys.path.insert(0, script_dir)

        try:
            with open(script_path, 'r', encoding='utf-8') as f:
                code = compile(f.read(), script_path, 'exec')
                exec(code, script_globals)
            self.log.emit(f"[脚本] 执行完成: {script_path}，采集 {len(collected_results)} 条")
        except Exception as e:
            import traceback
            error_msg = traceback.format_exc()
            self.log.emit(f"[错误] 脚本异常: {str(e)}\n{error_msg}")
            raise

    def stop(self):
        self._running = False


"""

"""