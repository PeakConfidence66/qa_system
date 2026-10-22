"""
爬虫模块 - 支持 xpath/css/regex + POST+JSON 接口
优化版：增强自动识别 + 支持JSON分页 + 更好日期解析 + 多线程并发
"""
import re
import requests
from curl_cffi import requests as curl_requests
import json
from bs4 import BeautifulSoup
from lxml import etree
from datetime import datetime
from PyQt6.QtCore import QThread, pyqtSignal
from urllib.parse import urljoin, urlparse
from dateutil import parser
from dateutil.parser import ParserError
import trafilatura
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from models import Notice
from config import DEFAULT_HEADERS


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

        # 安全类型转换，确保 self.days 为整型，避免类型比较引发异常
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
        # 线程锁保护共享状态
        self._lock = threading.Lock()
        self._completed_count = 0

    def _recalc_current_dates(self, current_list):
        """
        辅助函数：为手工 BS4 规则命中的列表元素统一计算 days_old 和标准格式化时间，
        防止时区误差和未来时间漏洞。
        """
        for item in current_list:
            # 调用你原生的 _parse_date 进行统一时间转换和 days_old 核算
            formatted_date, days_old, success = self._parse_date(item['date'])
            item['date'] = formatted_date if success else item['date']
            item['days_old'] = days_old

    def run(self):
        total = len(self.links)
        self.total_collected = 0
        self.total_after_filter = 0
        self.seen_urls = set()
        self._completed_count = 0

        self.log.emit(f"[配置] 包含关键词: {self.include_keywords}, 排除关键词: {self.exclude_keywords}")
        self.log.emit(f"[配置] 线程数: {self.max_workers}, 共 {total} 个链接")

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
                # 提交所有任务
                future_to_link = {
                    executor.submit(self._process_single_link, link_info): link_info
                    for link_info in self.links
                }

                # 处理完成的任务
                for future in as_completed(future_to_link):
                    if not self._running:
                        break

                    link_info = future_to_link[future]
                    try:
                        future.result()
                    except Exception as e:
                        url = link_info.get('url', '')
                        self.log.emit(f"[错误] 链接处理失败 {url}: {str(e)}")

                    # 更新进度
                    with self._lock:
                        self._completed_count += 1
                        self.progress.emit(self._completed_count, total)

        summary = {
            "total_collected": self.total_collected,
            "total_after_filter": self.total_after_filter,
            "days": self.days,
            "include_keywords": ", ".join(self.include_keywords) if self.include_keywords else "(无)",
            "exclude_keywords": ", ".join(self.exclude_keywords) if self.exclude_keywords else "(无)",
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

        if not url:
            return

        self.log.emit(f"[开始] 学校: {school_name} | 学院: {college_name}")
        self.log.emit(f"[链接] {url}")

        try:
            count = self._crawl_with_pagination(url, school_name, college_name, rules)
            self.log.emit(f"[完成] 获取 {count} 条有效公告")
        except Exception as e:
            self.log.emit(f"[错误] {str(e)}")

    # ====================== 统一请求（支持 POST + JSON） ======================
    def _make_request(self, url, rules, page=1):
        """统一请求方法，支持 GET / POST + 增强编码处理 + curl_cffi 兜底"""
        method = rules.get("request_method", "GET").upper()
        headers = DEFAULT_HEADERS.copy()

        # ====================== 第一次尝试：普通 requests ======================
        try:
            if method == "POST":
                post_data = rules.get("request_data", {}).copy()
                # 自动处理分页参数
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

            # 状态码正常则直接返回
            if resp is not None and 200 <= resp.status_code < 300:
                self.log.emit(
                    f"[请求成功] {method} {url} | 编码: {resp.encoding} | 状态码: {resp.status_code} (requests)")
                return self._process_response(resp)  # 建议抽出来下面统一处理编码

        except Exception as e:
            self.log.emit(f"[普通requests失败] {method} {url} -> {str(e)}")

        # ====================== 兜底：使用 curl_cffi ======================
        self.log.emit(f"[切换curl_cffi重试] {method} {url}")
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

            if resp is None:
                return None

            self.log.emit(f"[curl_cffi成功] {method} {url} | 编码: {resp.encoding} | 状态码: {resp.status_code}")
            return self._process_response(resp)
        except Exception as e:
            self.log.emit(f"[curl_cffi也失败] {method} {url} -> {str(e)}")
            return None

        # ==================== 统一编码处理 ====================
        return self._process_response(resp)

    def _process_response(self, resp):
        """统一处理响应编码（兼容 requests 和 curl_cffi）"""
        if resp is None:
            return None

        # ==================== 编码处理 ====================
        encoding = None

        # 1. 先尝试 requests 原生方式
        if hasattr(resp, 'apparent_encoding') and resp.apparent_encoding:
            encoding = resp.apparent_encoding

        # 2. 如果没有或不理想，尝试从 headers 中获取 charset
        if not encoding or encoding.lower() in ['iso-8859-1', 'ascii']:
            content_type = resp.headers.get('Content-Type', '')
            if 'charset=' in content_type:
                try:
                    encoding = content_type.split('charset=')[-1].strip().lower()
                except:
                    pass

        # 3. 兜底策略（重点处理中文网站）
        if not encoding or encoding.lower() in ['iso-8859-1', 'ascii', '']:
            content = resp.content
            for enc in ['utf-8', 'gbk', 'gb2312', 'gb18030']:
                try:
                    text = content.decode(enc, errors='replace')
                    # 检测是否包含较多中文字符
                    if len(text) > 100 and any('\u4e00' <= c <= '\u9fff' for c in text[:500]):
                        encoding = enc
                        break
                except:
                    continue

        # 最终兜底
        if not encoding:
            encoding = 'utf-8'

        resp.encoding = encoding

        self.log.emit(f"[编码处理完成] 最终编码: {resp.encoding} | 状态码: {resp.status_code}")
        return resp

    # ====================== 翻页主逻辑 ======================
    def _crawl_with_pagination(self, start_url, school_name, college_name, rules):
        """带翻页采集 - 优化去重逻辑与时间拦截"""
        current_url = start_url
        page_count = 0
        total_items = 0
        seen_urls = set()  # 改用 URL 去重（更可靠）
        seen_titles_this_run = set()  # 本次爬取的标题记录（用于重复预警）
        consecutive_low_new = 0

        while current_url and self._running:
            page_count += 1

            self.log.emit(f"[翻页] 第 {page_count} 页 → {current_url}")

            response = self._make_request(current_url, rules, page=page_count)
            if not response:
                break

            html = response.text

            if rules.get("response_type") == "json":
                items = self._parse_json_response(html, current_url, rules)
            else:
                items = self._extract_items(html, current_url, rules)

            new_count = 0
            page_items = 0
            consecutive_old = 0  # 连续过旧的公告计数器
            should_stop_pagination = False  # 是否终止外层翻页的标志

            for item in items:
                if not self._running:
                    break

                title = item.get('title', '').strip()
                link = item.get('url', '')
                date_text = item.get('date', '')

                if not title or len(title) < 6:
                    continue

                # ==================== 改进的去重逻辑 ====================
                if link and link in seen_urls:
                    continue

                if title in seen_titles_this_run and page_count > 1:
                    continue

                seen_urls.add(link)
                seen_titles_this_run.add(title)

                new_count += 1

                # 解析日期，返回格式化时间、天数差、以及解析是否成功
                publish_date, days_old, is_valid_date = self._parse_date(date_text)

                # ==================== 改进的时间范围拦截 ====================
                if self.days > 0:
                    if not is_valid_date:
                        # 日期无法解析时，在开启筛选时安全跳过，但不累加连续超期数，防止误触提前终止抓取的条件
                        self.log.emit(f"[跳过] 无法解析日期 ({date_text})，在时间限制下安全过滤该公告")
                        continue

                    if days_old > self.days:
                        consecutive_old += 1
                        # 连续 5 条真实超期的公告，才认为不是置顶，从而真正终止抓取
                        if consecutive_old >= 5:
                            self.log.emit(f"[停止] 连续 {consecutive_old} 条公告超出 {self.days} 天范围，终止本项抓取")
                            should_stop_pagination = True
                            break
                        else:
                            self.log.emit(
                                f"[跳过] 超出天数范围 ({days_old}天前)，疑似置顶或解析偏差，继续检查后续公告...")
                            continue
                    else:
                        consecutive_old = 0  # 遇到新鲜文章，重置计数器

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
                    with self._lock:
                        self.total_after_filter += 1
                    page_items += 1

            # 如果内层循环判断需要完全停止，则跳出外层翻页
            if should_stop_pagination:
                break

            total_items += page_items
            self.log.emit(f"[本页] 新增有效内容 {new_count} 条，入库 {page_items} 条")

            # 如果连续 2 页新增内容很少，就停止
            if new_count <= 2:
                consecutive_low_new += 1
                if consecutive_low_new >= 2:
                    self.log.emit(f"[停止] 连续 {consecutive_low_new} 页新增内容极少，终止翻页")
                    break
            else:
                consecutive_low_new = 0

            next_url = self._get_next_page_url(html, current_url, rules)
            if not next_url or next_url == current_url:
                self.log.emit(f"[停止] 未找到下一页，正常结束翻页")
                break

            current_url = next_url

        self.log.emit(f"[完成] {school_name} / {college_name} 共翻 {page_count} 页，采集 {total_items} 条")
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

            self.log.emit(f"[JSON解析] 成功提取 {len(items)} 条")
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

        for txt in ['下一页', '下一頁', '下页', '后一页', '›', '>']:
            a = soup.find('a', string=lambda s: s and txt in str(s))
            if a and a.get('href'):
                next_url = urljoin(current_url, a.get('href'))
                if next_url != current_url:
                    self.log.emit(f"[翻页] 文字下一页 → {next_url}")
                    return next_url

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
                        self.log.emit(f"[翻页] URL模式推断 → {next_url}")
                        return next_url
                except:
                    continue

        self.log.emit("[翻页] 未检测到下一页")
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
                next_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{new_query}"

                self.log.emit(f"[翻页] page参数递增 → 第 {next_page} 页 {next_url}")
                return next_url
            except:
                pass

        if 'page' not in query_params:
            next_url = current_url + ('&' if '?' in current_url else '?') + 'page=2'
            self.log.emit(f"[翻页] 添加page参数 → 第2页 {next_url}")
            return next_url

        self.log.emit("[翻页] page参数处理失败")
        return None

    # ====================== 核心提取 ======================
    def _extract_items(self, html, base_url, rules):
        if rules.get("title_rule"):
            return self._parse_with_rules(html, base_url, rules)
        else:
            return self._auto_detect_parse(html, base_url)

    def _parse_with_rules(self, html, base_url, rules):
        title_type = rules.get("title_type", "xpath")
        if title_type == "xpath":
            return self._parse_xpath(html, base_url, rules.get("title_rule"),
                                     rules.get("link_rule"), rules.get("date_rule"))
        elif title_type == "css":
            return self._parse_css(html, base_url, rules.get("title_rule"),
                                   rules.get("link_rule"), rules.get("date_rule"))
        elif title_type == "regex":
            return self._parse_regex(html, base_url, rules.get("title_rule"),
                                     rules.get("link_rule"), rules.get("date_rule"))
        return []

    def _parse_xpath(self, html, base_url, title_rule, link_rule, date_rule):
        """使用XPath解析"""
        items = []
        try:
            tree = etree.HTML(html)

            # 获取标题列表
            titles = tree.xpath(title_rule) if title_rule else []

            # 获取链接列表（如果配置了链接选择器）
            links = []
            if link_rule:
                link_elems = tree.xpath(link_rule)
                for elem in link_elems:
                    href = ""
                    if hasattr(elem, 'get'):
                        href = elem.get('href', '')
                    elif isinstance(elem, str):
                        href = elem
                    if href:
                        links.append(urljoin(base_url, href))

            # 获取日期列表
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
                title = ""
                url = ""

                if isinstance(title_elem, str):
                    title = title_elem.strip()
                elif hasattr(title_elem, 'text'):
                    title = (title_elem.text or "").strip()
                elif hasattr(title_elem, 'xpath'):
                    title = (title_elem.xpath('string(.)') or "").strip()

                # 获取链接
                if i < len(links):
                    url = links[i]
                else:
                    # 从标题元素中提取链接
                    if hasattr(title_elem, 'get'):
                        href = title_elem.get('href', '')
                        if href:
                            url = urljoin(base_url, href)
                        else:
                            a = title_elem.xpath('.//a[@href]')
                            if a:
                                url = urljoin(base_url, a[0].get('href', ''))

                date_text = dates[i] if i < len(dates) else ""

                if title:
                    items.append({"title": title, "url": url, "date": date_text})

        except Exception as e:
            self.log.emit(f"[警告] XPath解析失败: {str(e)}")

        return items

    def _parse_css(self, html, base_url, title_rule, link_rule, date_rule):
        """使用CSS选择器解析"""
        items = []
        try:
            soup = BeautifulSoup(html, 'html.parser')

            # 获取标题列表
            title_elems = soup.select(title_rule) if title_rule else []

            # 获取链接列表（如果配置了链接选择器）
            links = []
            if link_rule:
                link_elems = soup.select(link_rule)
                for elem in link_elems:
                    if elem.get('href'):
                        links.append(urljoin(base_url, elem.get('href')))

            for i, title_elem in enumerate(title_elems):
                title = title_elem.get_text(strip=True)

                # 获取链接
                url = ""
                if i < len(links):
                    url = links[i]
                else:
                    # 从标题元素中提取链接
                    a = title_elem if title_elem.name == 'a' else title_elem.find('a')
                    if a and a.get('href'):
                        url = urljoin(base_url, a.get('href'))

                # 获取日期
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
            self.log.emit(f"[警告] CSS解析失败: {str(e)}")

        return items

    def _parse_regex(self, html, base_url, title_rule, link_rule, date_rule):
        """使用正则表达式解析"""
        items = []
        try:
            # 标题正则
            title_matches = re.findall(title_rule, html) if title_rule else []

            # 链接正则
            link_matches = []
            if link_rule:
                link_matches = re.findall(link_rule, html)

            # 日期正则
            date_matches = []
            if date_rule:
                date_matches = re.findall(date_rule, html)

            for i, title in enumerate(title_matches):
                if isinstance(title, tuple):
                    title = title[0] if title else ""

                title = re.sub(r'<[^>]+>', '', str(title)).strip()

                # 获取链接
                url = ""
                if i < len(link_matches):
                    link = link_matches[i]
                    if isinstance(link, tuple):
                        link = link[0] if link else ""
                    url = urljoin(base_url, str(link))
                else:
                    # 尝试从标题中提取链接
                    href_match = re.search(r'href=["\']([^"\']+)["\']', str(title_matches[i]))
                    if href_match:
                        url = urljoin(base_url, href_match.group(1))

                date_text = ""
                if i < len(date_matches):
                    d = date_matches[i]
                    if isinstance(d, tuple):
                        d = d[0] if d else ""
                    date_text = re.sub(r'<[^>]+>', '', str(d)).strip()

                if title:
                    items.append({"title": title, "url": url, "date": date_text})

        except Exception as e:
            self.log.emit(f"[警告] Regex解析失败: {str(e)}")

        return items

    # ===============gerapy_auto_extractor 时间匹配函数=============
    def _extract_date_for_gerapy_item(self, a_tag, title):
        """
        专为 Gerapy 匹配到的 <a> 标签寻找其相对应的日期。
        采用【自内向外局部搜索】+【通用标题消除正则法】双层对齐机制。（已清除冗余块并新增容器识别）
        """
        if not a_tag:
            return ""

        # ==================== 第一阶段：行级容器对齐与回溯 ====================
        parent_container = None
        curr = a_tag
        for _ in range(5):  # 向上探查最多 5 层
            if curr is None or curr.name == 'body':
                break
            curr_class = ''.join(curr.get('class', [])).lower()
            curr_id = curr.get('id', '').lower()
            # 【重点升级】：添加 'wrap' 和 'column'，使其能在第二步瞬间锁死 .columnNew-wrap 容器
            if curr.name in ['li', 'tr', 'dd'] or \
                    any(x in curr_class for x in ['item', 'line_u', 'row', 'columns', 'col-', 'wrap', 'column']) or \
                    any(x in curr_id for x in ['item', 'line_u', 'row', 'columns', 'wrap', 'column']):
                parent_container = curr
                break
            curr = curr.parent

        if not parent_container:
            parent_container = a_tag.parent

        # ==================== 新增特异对齐：东华大学等 news_day + news_year 强拆匹配 ====================
        # 在这里直接进行类名特征级比对，绕过复杂的文本replace流程，实现 100% 精准拦截
        if parent_container:
            # 兼容多种常见的日/年拆分类名组合
            day_span = parent_container.select_one('.news_day, .news-day, .date-day, .day')
            year_span = parent_container.select_one('.news_year, .news-year, .date-year, .year, .year-month')
            if day_span and year_span:
                day_txt = day_span.get_text(strip=True).strip()
                ym_txt = year_span.get_text(strip=True).replace('/', '-').replace('.', '-').strip()
                # 校验：day_txt 为数字，ym_txt 为类似 2026-05 格式
                if day_txt.isdigit() and re.match(r'^\d{4}[-.]\d{1,2}$', ym_txt):
                    # 标准化补零输出，如 2026-05-21 或 2022-10-21
                    return f"{ym_txt}-{day_txt.zfill(2)}"

        # ==================== 第二阶段：【标题消除 + 局部正则匹配】核心 ====================
        if parent_container and title:
            # 1. 拿到整行容器的纯文本（此时会包含标题、真实日期、和可能污染标题的杂质数字）
            row_text = parent_container.get_text(" ", strip=True)
            # 2. 核心大招：在文本中用空字符擦除掉提取出的标题本身！
            remaining_text = row_text.replace(title, "").strip()
            # 3. 清理掉边缘常见修饰噪声
            remaining_text = re.sub(r'[\s\xa0\u3000\n\r|｜\[\]\(\)（）>›»▪•*◎：:：\-]+', ' ', remaining_text).strip()

            # 4. 直接使用最精细的正则表达式，在剩下的干净文本中提取日期：

            # 模式 A：标准的 YYYY-MM-DD / YYYY.MM.DD / YYYY/MM/DD 等
            m_full = re.search(r'(\d{4})[-_./\s](\d{1,2})[-_./\s](\d{1,2})', remaining_text)
            if m_full:
                return f"{m_full.group(1)}-{m_full.group(2).zfill(2)}-{m_full.group(3).zfill(2)}"

            # 模式 B：搜索日先、年月在后（如：08 2026.05）的拆分型非标日期
            m_day_first = re.search(r'\b(\d{1,2})\s+(\d{4})[-_./\s](\d{1,2})\b', remaining_text)
            if m_day_first:
                return f"{m_day_first.group(2)}-{m_day_first.group(3).zfill(2)}-{m_day_first.group(1).zfill(2)}"

            # 模式 C：搜索年份在末尾的日期 MM-DD YYYY
            m_end_year = re.search(r'(\d{1,2})[-_./\s](\d{1,2})[-_./\s](\d{4})', remaining_text)
            if m_end_year:
                return f"{m_end_year.group(3)}-{m_end_year.group(1).zfill(2)}-{m_end_year.group(2).zfill(2)}"

            # 模式 D：搜索带中文的 4 位年份日期 YYYY年MM月DD日
            m_cn = re.search(r'(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})', remaining_text)
            if m_cn:
                return f"{m_cn.group(1)}-{m_cn.group(2).zfill(2)}-{m_cn.group(3).zfill(2)}"

            # 模式 E：搜索 2 位数年份的日期 YY-MM-DD / YY/MM/DD（进行 20YY 规范补全）
            m_short_year = re.search(r'\b(\d{2})[-_./](\d{1,2})[-_./](\d{1,2})\b', remaining_text)
            if m_short_year:
                return f"20{m_short_year.group(1)}-{m_short_year.group(2).zfill(2)}-{m_short_year.group(3).zfill(2)}"

            # 模式 F：搜索无年份的短日期 MM-DD
            m_short = re.search(r'\b(\d{1,2})[-_./](\d{1,2})\b', remaining_text)
            if m_short:
                return f"{m_short.group(1)}-{m_short.group(2)}"

        # ==================== 第三阶段：运行通用提取器兜底 ====================
        if parent_container:
            date_text = self._extract_date_from_element(parent_container)
            if date_text:
                return date_text

        return ""

    # ====================== 新增：微观 DOM 邻近分裂日期重组器 ======================
    def _extract_split_date_from_container(self, container, title):
        """
        微观 DOM 邻近分裂日期对齐算法
        在锁定的单行容器内，剔除标题文本干扰，智能提取并重组散落的 [年-月] 与 [日] 标签。
        """
        if not container:
            return None

        # 1. 提取容器内所有的文本节点，并物理抠除标题文本，防止标题内的数字干扰日期匹配
        text_nodes = []
        for element in container.find_all(text=True):
            text_str = element.strip()
            if not text_str:
                continue
            if title and (text_str in title or title in text_str):
                continue
            # 过滤掉常见的无用噪点字符
            text_str = re.sub(r'[\s\xa0\u3000\r\n|｜\[\]\(\)（）>›»▪•*◎：:：\-]+', ' ', text_str).strip()
            if text_str:
                text_nodes.append(text_str)

        # 合并文本，并用空格分隔
        combined_text = " ".join(text_nodes)

        # 2. 尝试标准连续日期提取 (如 "2021-06-25" 或 "2021/06/25" 或 "2021年06月25日")
        m_standard = re.search(r'\b(20\d{2})\s*[-/年.]\s*([01]?\d)\s*[-/月.]\s*([0-3]?\d)\s*日?\b', combined_text)
        if m_standard:
            return f"{m_standard.group(1)}-{int(m_standard.group(2)):02d}-{int(m_standard.group(3)):02d}"

        # 3. 智能拆分检测
        year_month = None
        day = None

        # 3.1 寻找 [年-月] 结构 (如 "2021-06", "2021.06", "2021/06")
        for node in text_nodes:
            m_ym = re.search(r'\b(20\d{2})[-/.](\d{1,2})\b', node)
            if m_ym:
                year_month = f"{m_ym.group(1)}-{int(m_ym.group(2)):02d}"
                break

        # 3.2 寻找独立的 [天数] 结构 (长度为 1-2 位，且在 1-31 之间的数字)
        for node in text_nodes:
            # 排除已作为年月的节点本身
            if year_month and (node in year_month or any(p in node for p in year_month.split('-'))):
                continue
            m_day = re.search(r'\b([0-3]?\d)\b', node)
            if m_day:
                val = int(m_day.group(1))
                if 1 <= val <= 31:
                    day = f"{val:02d}"
                    break

        # 3.3 如果成功拼合 [年-月] + [日]
        if year_month and day:
            return f"{year_month}-{day}"

        # 3.4 另一种分裂情况：独立4位 [年份] (2021) + [月-日] (06-25)
        year_only = None
        month_day = None

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

    # ====================== 自动识别（HTML） ======================
    def _auto_detect_parse(self, html, base_url):
        soup = BeautifulSoup(html, 'html.parser')
        items = []

        self.log.emit(f"[自动识别] 开始分析 {base_url}")

        # ==================== 1. 第一主引擎：使用 gerapy_auto_extractor 提取 ====================
        try:
            from datetime import datetime
            from gerapy_auto_extractor import extract_list
            try:
                from gerapy_auto_extractor import extract_datetime
            except ImportError:
                try:
                    from gerapy_auto_extractor.extractors.datetime import extract_datetime
                except ImportError:
                    extract_datetime = None

            list_data = extract_list(html)
            gerapy_items = list_data.get('items', []) if isinstance(list_data, dict) else (list_data or [])

            if gerapy_items and len(gerapy_items) >= 3:
                self.log.emit(f"[成功] gerapy_auto_extractor 识别出列表 → 共 {len(gerapy_items)} 条")

                current = []
                for item in gerapy_items:
                    title = (item.get('title') or '').strip()
                    link = (item.get('url') or item.get('link') or '').strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    # 【安全防线 1：拦截导航与列表页噪声】
                    if not link or any(p in link.lower() for p in
                                       ['javascript:', '#', 'mailto:', 'szll', 'xyld', 'xygk', 'index.htm',
                                        'index.html', 'main.htm', 'list.htm', 'list.html']):
                        continue

                    full_url = urljoin(base_url, link) if link else ''

                    # 【安全防线 2：拦截非标准协议】
                    if not full_url or not any(full_url.lower().startswith(p) for p in ['http://', 'https://']):
                        continue

                    # 1. 优先获取 Gerapy 的原日期
                    date_text = (item.get('date') or item.get('time') or '').strip()

                    # 检测是否为格式完整的 YYYY-MM-DD
                    is_complete_date = False
                    if date_text:
                        if re.search(r'\d{4}[-/年.]\d{1,2}[-/月.]\d{1,2}', date_text) or re.match(r'^\d{8}$',
                                                                                                  date_text):
                            is_complete_date = True

                    # 2. 如果日期缺失或不完整（触发了您提到的分裂情况，如仅提取到 "2021-06"），启动高精密本地对齐
                    if (not is_complete_date) and link:
                        a_tag = None
                        target_abs_url = urljoin(base_url, link) if link else ''

                        if target_abs_url:
                            # 2.1 高精密反向匹配定位 A 标签
                            for possible_a in soup.find_all('a', href=True):
                                possible_href = possible_a.get('href', '').strip()
                                if possible_href.startswith(('javascript:', '#')):
                                    continue
                                if urljoin(base_url, possible_href) == target_abs_url:
                                    a_tag = possible_a
                                    break

                            # 2.2 备用末尾路径比对
                            if not a_tag:
                                target_filename = link.split('/')[-1] if '/' in link else link
                                if target_filename and len(target_filename) > 5:
                                    for possible_a in soup.find_all('a', href=True):
                                        possible_href = possible_a.get('href', '').strip()
                                        if target_filename in possible_href:
                                            a_tag = possible_a
                                            break

                            # 2.3 锁定安全的单行行容器（防止漂移到大容器）
                            if a_tag:
                                parent_container = None
                                curr = a_tag
                                for _ in range(5):
                                    if curr is None or curr.name == 'body':
                                        break

                                    # 核心防线：如果当前节点含有 2 个及以上不同的详情页链接，说明爬出了单行范围，立刻停止
                                    sibling_links = []
                                    for a in curr.find_all('a', href=True):
                                        h = a.get('href', '').strip()
                                        if h and not h.startswith(
                                                ('javascript:', '#', 'mailto:')) and 'list' not in h.lower():
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

                                if not parent_container:
                                    parent_container = a_tag.parent

                                # 2.4 在锁定的行级闭包内进行【多级合并提取】
                                if parent_container:
                                    # 策略 A：利用微观 DOM 分裂日期重组器（将散落的 年、月、日 标签内容安全拼接）
                                    reconstructed = self._extract_split_date_from_container(parent_container, title)
                                    if reconstructed:
                                        date_text = reconstructed
                                        is_complete_date = True
                                        self.log.emit(
                                            f"[对齐成功-分裂重组] 成功拼合局部断裂日期: {date_text} ({title[:15]}...)")

                                    # 策略 B：若未成功拼合，使用自定义基础时间正则提取器兜底
                                    if not is_complete_date:
                                        temp_date = self._extract_date_for_gerapy_item(parent_container, title)
                                        if temp_date and (
                                                re.search(r'\d{4}[-/年.]\d{1,2}[-/月.]\d{1,2}', temp_date) or re.match(
                                            r'^\d{8}$', temp_date)):
                                            date_text = temp_date
                                            is_complete_date = True

                                    # 策略 C：使用 gerapy.extract_datetime 本地解析行级 HTML 片段
                                    if (not is_complete_date) and extract_datetime:
                                        try:
                                            raw_extracted = extract_datetime(str(parent_container))
                                            if raw_extracted:
                                                if isinstance(raw_extracted, datetime):
                                                    date_text = raw_extracted.strftime('%Y-%m-%d %H:%M:%S')
                                                else:
                                                    date_text = str(raw_extracted)
                                                is_complete_date = True
                                        except Exception as e:
                                            self.log.emit(f"[局部容器提取异常]: {e}")

                    # 3. 最终解析标准化
                    if date_text:
                        if isinstance(date_text, datetime):
                            date_text = date_text.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            date_text = str(date_text).strip()

                    formatted_date, days_old, success = self._parse_date(date_text)

                    # ==================== 3. 第三主引擎：【详情页网络探测】 ====================
                    # 如果经过列表页智能分析仍未拿到有效日期，且不是栏目页，才启动详情页探测
                    if not success and full_url and any(
                            full_url.lower().startswith(p) for p in ['http://', 'https://']):
                        self.log.emit(f"[详情页探测] 列表页未识别到完整日期，尝试请求详情页获取: {full_url}")
                        try:
                            import requests
                            headers = {
                                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                            }
                            response = requests.get(full_url, headers=headers, timeout=6)
                            response.encoding = response.apparent_encoding or 'utf-8'

                            detail_html = response.text
                            if detail_html:
                                detail_date = None
                                if extract_datetime:
                                    try:
                                        raw_detail_date = extract_datetime(detail_html)
                                        if raw_detail_date:
                                            if isinstance(raw_detail_date, datetime):
                                                detail_date = raw_detail_date.strftime('%Y-%m-%d %H:%M:%S')
                                            else:
                                                detail_date = str(raw_detail_date)
                                            self.log.emit(f"[详情页探测成功] gerapy 识别出详情页日期: {detail_date}")
                                    except Exception as e:
                                        self.log.emit(f"[详情页探测异常] gerapy 提取错误: {e}")

                                if not detail_date:
                                    try:
                                        from htmldate import find_date
                                        detail_date = find_date(detail_html)
                                        if detail_date:
                                            self.log.emit(f"[详情页探测成功] htmldate 识别出详情页日期: {detail_date}")
                                    except ImportError:
                                        pass

                                if detail_date:
                                    date_text = detail_date
                                    formatted_date, days_old, success = self._parse_date(date_text)
                        except Exception as e:
                            self.log.emit(f"[详情页探测失败] 访问异常: {e}")

                    # 清洗标题中可能连带抓取进来的日期
                    safe_date_text = str(date_text) if date_text else ""
                    safe_formatted_date = str(formatted_date) if formatted_date else ""

                    if safe_date_text and safe_date_text in title:
                        title = title.replace(safe_date_text, "").strip()
                    if safe_formatted_date and safe_formatted_date in title:
                        title = title.replace(safe_formatted_date, "").strip()
                    title = title.strip('- \t\n\r|｜[]()（）')

                    current.append({
                        "title": title,
                        "url": full_url,
                        "date": formatted_date if success else date_text,
                        "days_old": days_old
                    })

                if len(current) >= 3:
                    self.log.emit(f"[对齐成功] 自动提取器成功匹配 {len(current)} 条结构化数据")
                    return current

        except ImportError:
            self.log.emit("[警告] gerapy_auto_extractor 未安装，跳过智能自动提取")
        except Exception as e:
            self.log.emit(f"[gerapy] 提取失败: {e}，将使用备用规则")

        # ==================== 2. 新增适配：脚本 JSON 数据嵌入格式 (dataList 嵌入式) ====================
        script_tags = soup.find_all('script')
        for script in script_tags:
            script_text = script.get_text()
            if 'dataList' in script_text:
                start_idx = script_text.find('dataList')
                if start_idx != -1:
                    bracket_start = script_text.find('[', start_idx)
                    if bracket_start != -1:
                        bracket_count = 0
                        bracket_end = -1
                        for i in range(bracket_start, len(script_text)):
                            char = script_text[i]
                            if char == '[':
                                bracket_count += 1
                            elif char == ']':
                                bracket_count -= 1
                                if bracket_count == 0:
                                    bracket_end = i
                                    break

                        if bracket_end != -1:
                            json_str = script_text[bracket_start:bracket_end + 1]
                            try:
                                import json
                                data_list = json.loads(json_str)
                                current = []
                                for block in data_list:
                                    infolist = block.get('infolist', [])
                                    for info in infolist:
                                        title = (info.get('title') or info.get('infotitle') or '').strip()
                                        link = (info.get('url') or info.get('sourceurl') or '').strip()

                                        if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                                            continue

                                        full_url = urljoin(base_url, link) if link else ''

                                        # 日期提取：高优读取 daytime，低优读取时间戳
                                        date_text = (info.get('daytime') or '').strip()
                                        if not date_text:
                                            ts = info.get('releaseTime') or info.get('releasetime') or info.get(
                                                'createtime')
                                            if ts:
                                                try:
                                                    ts_val = int(ts)
                                                    if len(str(ts_val)) >= 13:
                                                        dt_obj = datetime.fromtimestamp(ts_val / 1000)
                                                    else:
                                                        dt_obj = datetime.fromtimestamp(ts_val)
                                                    date_text = dt_obj.strftime('%Y-%m-%d')
                                                except:
                                                    pass

                                        formatted_date, days_old, success = self._parse_date(date_text)

                                        # 清洗标题中可能带有的日期干扰
                                        if date_text and date_text in title:
                                            title = title.replace(date_text, "").strip()
                                        if formatted_date and formatted_date in title:
                                            title = title.replace(formatted_date, "").strip()
                                        title = title.strip('- \t\n\r|｜[]()（）')

                                        current.append({
                                            "title": title,
                                            "url": full_url,
                                            "date": formatted_date if success else date_text,
                                            "days_old": days_old
                                        })
                                if len(current) >= 3:
                                    self.log.emit(
                                        f"[成功] 脚本 JSON 嵌入式列表 (dataList) 策略定位命中 → {len(current)} 条")
                                    return current
                            except Exception as ex:
                                self.log.emit(f"[警告] 解析 dataList 脚本 JSON 失败: {ex}")

        # -------------------- 规则：辽宁师大等 div.item / h2 + .date (span+text) 复合卡片结构 --------------------
        lnnu_items = soup.select('.item, div.item')
        if not lnnu_items:
            # 兜底：通过 Python 循环精准匹配含有 .date 且含有 h2 标题的 div.item 节点
            lnnu_items = [div for div in soup.find_all('div', class_='item') if
                          div.select_one('.date') and div.select_one('h2')]

        if lnnu_items:
            # 校验首个元素特征，本身含有 h2 标题与 .date 容器
            first_item = lnnu_items[0]
            if first_item.select_one('h2') and first_item.select_one('.date'):
                current = []
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for div in lnnu_items:
                    a = div.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题（精准锁定 h2 容器）
                    h2 = div.select_one('h2')
                    title = h2.get_text(strip=True) if h2 else a.get('title', '').strip()
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：重组 .date 内部的 span (天，并进行补零) 与其余文本 (年-月) -> YYYY-MM-DD
                    date_text = ""
                    date_div = div.select_one('.date')
                    if date_div:
                        span_day = date_div.select_one('span')
                        full_text = date_div.get_text(strip=True)  # 连读文本如 "18/2026-05" 或 "18/  2026-05"
                        if span_day:
                            day = span_day.get_text(strip=True)
                            # 从连读文本中刨除天数、斜杠及空格
                            year_month = full_text.replace(day, "").replace('/', '').replace(' ', '').strip()
                            if day and year_month:
                                date_text = f"{year_month}-{day.zfill(2)}"

                    if not date_text:
                        date_text = self._extract_date_from_element(div)

                    current.append({"title": title, "url": url, "date": date_text})

                # 准入门槛设为 >= 1，完美保障在数据项较少时亦可一击制胜
                if len(current) >= 1:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 辽宁师大 .item (h2+date) 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：清华研招网等 a[data-val] 动态拼接 URL 复合列表结构 --------------------
        # 该结构中，标准 href 是无用的 javascript:void(0);，真实链接需要拼接特定的基准前缀和 a.get('data-val') 值
        ts_yzbm_items = []
        for li in soup.find_all('li'):
            a = li.find('a')
            if a and a.get('data-val'):
                # 检测核心容器
                if a.select_one('.time') and a.select_one('.name'):
                    ts_yzbm_items.append(li)

        if ts_yzbm_items:
            current = []

            for li in ts_yzbm_items:
                a = li.find('a')
                if not a:
                    continue

                # 1. 标题提取：直接提取 .name 容器文本
                name_div = a.select_one('.name')
                title = name_div.get_text(strip=True) if name_div else a.get('title', '').strip()
                title = re.sub(r'[…\s\.]+$', '', title).strip()

                if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                    continue

                # 2. 链接重组：提取 data-val 属性值并动态拼接目标基准前缀
                data_val = a.get('data-val', '').strip()
                if not data_val:
                    continue
                # 基准拼接地址格式：https://yzbm.tsinghua.edu.cn/publish/s03/s0301/detail/ + data_val [1]
                url = f"https://yzbm.tsinghua.edu.cn/publish/s03/s0301/detail/{data_val}"

                # 3. 日期提取：直接读取 .time 容器内的文本，如 "2026-05-27"
                date_text = ""
                time_div = a.select_one('.time')
                if time_div:
                    date_text = time_div.get_text(strip=True)

                if not date_text:
                    date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            # 高精度手工规则，将返回阈值设为 >= 1，完美保障在数据项较少时亦可一击制胜
            if len(current) >= 1:
                self._recalc_current_dates(current)
                self.log.emit(f"[手工规则] 清华 yzbm.tsinghua (data-val拼接) 策略定位命中 → {len(current)} 条")
                return current

        # -------------------- 规则：清华等 div.time(em+span) + div.name 复合卡片结构 --------------------
        thu_admission_items = []
        # 直接在全页检索含有 a 标签，且 a 内部同时包含 .time 和 .name 容器的行级 li 节点
        for li in soup.find_all('li'):
            a = li.find('a', href=True)
            if a:
                # 检测核心容器
                time_div = a.select_one('.time') or a.find('div', class_=lambda c: c and 'time' in str(c))
                name_div = a.select_one('.name') or a.find('div', class_=lambda c: c and 'name' in str(c))
                if time_div and name_div:
                    # 校验时间容器是否同时包含 em 和 span
                    if time_div.find('em') and time_div.find('span'):
                        thu_admission_items.append(li)

        if thu_admission_items:
            current = []
            base_tag = soup.find('base', href=True)
            local_base_url = base_tag['href'].strip() if base_tag else base_url

            for li in thu_admission_items:
                a = li.find('a', href=True)
                if not a:
                    continue

                # 提取标题（优先从 div.name 中直接提取标题文本，防范 title 属性截断）
                name_div = a.select_one('.name') or a.find('div', class_=lambda c: c and 'name' in str(c))
                title = name_div.get_text(strip=True) if name_div else a.get('title', '').strip()
                title = re.sub(r'[…\s\.]+$', '', title).strip()

                if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(local_base_url, a.get('href', '').strip())

                # 日期提取：重组 time 内部的 span (年-月) 和 em (天，并对天补零) -> YYYY-MM-DD
                date_text = ""
                time_div = a.select_one('.time') or a.find('div', class_=lambda c: c and 'time' in str(c))
                if time_div:
                    em_tag = time_div.find('em')
                    span_tag = time_div.find('span')
                    if em_tag and span_tag:
                        day = em_tag.get_text(strip=True)
                        year_month = span_tag.get_text(strip=True).replace('.', '-')
                        if day and year_month:
                            date_text = f"{year_month}-{day.zfill(2)}"

                # 兜底
                if not date_text:
                    date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            # 高精度手工规则，将返回阈值设为 >= 1，完美保障在数据项较少时亦可一击制胜
            if len(current) >= 1:
                self._recalc_current_dates(current)
                self.log.emit(
                    f"[手工规则] 清华 yz.tsinghua.edu.cn (time/name) 策略定位命中 → {len(current)} 条")
                return current

        # -------------------- 规则：八一农大等 .column-news-item 自关联卡片结构 --------------------
        byau_items = soup.select('a.column-news-item, .column-news-item')
        if not byau_items:
            # 兜底：精确寻找 class 包含 column-news-item 的所有 a 节点
            byau_items = [a for a in soup.find_all('a') if 'column-news-item' in a.get('class', [])]

        if byau_items:
            # 校验首个元素特征，本身是 a 标签且包含 .column-news-title 和 .column-news-date 容器
            first_item = byau_items[0]
            if first_item.select_one('.column-news-title') and first_item.select_one('.column-news-date'):
                current = []
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for a in byau_items:
                    if a.name != 'a' or not a.get('href'):
                        continue

                    # 提取标题
                    title_span = a.select_one('.column-news-title')
                    title = title_span.get_text(strip=True) if title_span else a.get('title', '').strip()
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：直接读取 .column-news-date 属性值，如 "2026-05-22"
                    date_text = ""
                    date_span = a.select_one('.column-news-date')
                    if date_span:
                        date_text = date_span.get_text(strip=True)

                    if not date_text:
                        date_text = self._extract_date_from_element(a)

                    current.append({"title": title, "url": url, "date": date_text})

                # 准入门槛设为 >= 1，完美保障在数据项较少时亦可一击制胜
                if len(current) >= 1:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 八一农大 .column-news-item 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：三博脑科 .newstitem / span.md + span.year 拆分自关联卡片结构 --------------------
        sbnk_items = soup.select('.newstitem, div.newstitem')
        if not sbnk_items:
            # 兜底：精准筛选所有含有 .newscontent 且含有 p.date 内部 span.md 容器的 newstitem 卡片
            sbnk_items = [div for div in soup.find_all('div') if
                          'newstitem' in div.get('class', []) and div.select_one('.date span.md')]

        if sbnk_items:
            # 校验首个元素特征，本身包含 .newscontent 且包含 .newsbody p.title 与 p.date 容器
            first_item = sbnk_items[0]
            if first_item.select_one('.date .md') and first_item.select_one(
                    '.date .year') and first_item.select_one('p.title'):
                current = []
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for div in sbnk_items:
                    a = div.select_one('a.newscontent') or div.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题
                    title_p = a.select_one('p.title')
                    title = title_p.get_text(strip=True) if title_p else a.get('title', '').strip()
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：重组 date 内部的 span.md (年份，如 2026) 和 span.year (月-日，如 01-21) -> YYYY-MM-DD
                    date_text = ""
                    date_p = a.select_one('.date')
                    if date_p:
                        md_span = date_p.select_one('.md')
                        year_span = date_p.select_one('.year')
                        if md_span and year_span:
                            year = md_span.get_text(strip=True)
                            month_day = year_span.get_text(strip=True).replace('/', '-')
                            if year and month_day:
                                date_text = f"{year}-{month_day}"

                    if not date_text:
                        date_text = self._extract_date_from_element(div)

                    current.append({"title": title, "url": url, "date": date_text})

                # 高特异性手工规则，返回阈值设为 >= 1，完美保障在数据项较少时亦可一击制胜
                if len(current) >= 1:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 三博脑科 .newstitem 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：首师大等 checkXnwk 动态 A 标签卡片结构 --------------------
        # 此类结构将真实跳转链接存放在 onclick="checkXnwk('xxx.htm', ...)" 属性中，而 href 是无意义的 JS 占位符
        cnu_items = []
        for li in soup.find_all('li'):
            a = li.find('a')
            if a:
                onclick = a.get('onclick', '') or a.get('onClick', '')
                if onclick and 'checkXnwk' in str(onclick):
                    cnu_items.append(li)

        if cnu_items:
            current = []
            base_tag = soup.find('base', href=True)
            local_base_url = base_tag['href'].strip() if base_tag else base_url

            for li in cnu_items:
                a = li.find('a')
                if not a:
                    continue

                # 提取标题：优先读取 a 标签自带的 title 属性，防止文字被前端截断
                title = a.get('title', '').strip()
                if not title:
                    title = a.get_text(strip=True)
                title = re.sub(r'[…\s\.]+$', '', title).strip()

                if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                    continue

                # 提取链接：解析包含在自定义函数 checkXnwk 中的第一个参数文件名
                url = ""
                onclick = a.get('onclick', '') or a.get('onClick', '')
                if onclick:
                    # 正则捕获单引号或双引号内的第一个文件名参数 (通常是 .htm 或 .html 结尾的文件)
                    match = re.search(r"checkXnwk\(\s*['\"](.*?)['\"]", str(onclick))
                    if match:
                        url = urljoin(local_base_url, match.group(1).strip())

                if not url:
                    continue

                # 日期提取：读取同级 span 容器，物理剥离可能带有的中括号 []
                date_text = ""
                span_date = li.find('span')
                if span_date:
                    date_text = span_date.get_text(strip=True).strip('[]')

                if not date_text:
                    date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            # 高特异性手工规则，将准入返回门槛设为 >= 1，完美保障在数据项较少时亦可一击制胜
            if len(current) >= 1:
                self._recalc_current_dates(current)
                self.log.emit(f"[手工规则] 首师大 checkXnwk 动态跳转策略定位命中 → {len(current)} 条")
                return current

        # -------------------- 规则：青海民大等 .wp_articlecontent 裸 LI 自关联卡片结构 --------------------
        # 针对部分采用 WebPlus 系统、不规范地省略了 <ul> 容器直接放置 <li> 的特殊列表
        wp_list_items = []
        for li in soup.find_all('li'):
            a = li.find('a', href=True)
            if a:
                # 校验核心卡片特征：a 标签内必须同时包含带 'listing' 类名的标题 span，以及带 'date' 类名的日期 span
                span_title = a.find('span', class_=lambda c: c and 'listing' in str(c))
                span_date = a.find('span', class_=lambda c: c and 'date' in str(c))
                if span_title and span_date:
                    wp_list_items.append(li)

        if wp_list_items:
            current = []
            base_tag = soup.find('base', href=True)
            local_base_url = base_tag['href'].strip() if base_tag else base_url

            for li in wp_list_items:
                a = li.find('a', href=True)
                if not a:
                    continue

                # 提取标题（优先从 span.listing_lttkdmt 容器中提取）
                span_title = a.find('span', class_=lambda c: c and 'listing' in str(c))
                title = span_title.get_text(strip=True) if span_title else a.get('title', '').strip()
                title = re.sub(r'[…\s\.]+$', '', title).strip()

                if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(local_base_url, a.get('href', '').strip())

                # 日期提取：直接读取带有 'date' 类名的 span 容器文本，如 "2023-11-30"
                date_text = ""
                span_date = a.find('span', class_=lambda c: c and 'date' in str(c))
                if span_date:
                    date_text = span_date.get_text(strip=True)

                if not date_text:
                    date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            # 高特异性手工规则，将准入返回门槛设为 >= 1，完美保障在数据项较少时亦可一击制胜
            if len(current) >= 1:
                self._recalc_current_dates(current)
                self.log.emit(
                    f"[手工规则] 青海民大 .wp_articlecontent 裸LI卡片策略定位命中 → {len(current)} 条")
                return current

        # -------------------- 规则：鲁东大学等 .article-item / h5 + p.article-item-info 复合卡片结构 --------------------
        ldu_items = soup.select('.article-item, div.article-item')
        if not ldu_items:
            # 兜底：精确寻找 class 包含 article-item 且含有 data-index 的 div 节点
            ldu_items = [div for div in soup.find_all('div') if
                         'article-item' in div.get('class', []) and div.get('data-index')]

        if ldu_items:
            # 校验首个元素特征，本身是 div 且包含 .article-item-title 和 .article-item-info
            first_item = ldu_items[0]
            if first_item.select_one('.article-item-title') and first_item.select_one('.article-item-info'):
                current = []
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for div in ldu_items:
                    a = div.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题并降噪：复制 h5 标签，物理剥离内部的 img 图标节点，防止其 alt/src 污染标题文本
                    h5_title = a.select_one('.article-item-title') or a.find('h5')
                    if h5_title:
                        h5_copy = BeautifulSoup(str(h5_title), 'html.parser').find('h5')
                        img_tag = h5_copy.select_one('img')
                        if img_tag:
                            img_tag.decompose()  # 剔除复制出来的图标节点
                        title = h5_copy.get_text(strip=True)
                    else:
                        title = a.get_text(strip=True)

                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：直接读取 .article-item-info 类名下的标准日期文本，如 "2026年05月17日"
                    date_text = ""
                    info_p = div.select_one('.article-item-info') or div.find('p',
                                                                              class_=lambda c: c and 'info' in str(c))
                    if info_p:
                        date_text = info_p.get_text(strip=True)

                    if not date_text:
                        date_text = self._extract_date_from_element(div)

                    current.append({"title": title, "url": url, "date": date_text})

                # 高特异性手工规则，返回阈值设为 >= 1，完美保障在数据项较少时亦可一击制胜
                if len(current) >= 1:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 鲁东大学 .article-item 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：长春理工等 li[onClick] + p(标题) + a(日期) 复合列表结构 --------------------
        cust_items = []
        # 全页检索绑定了跳转事件且包含标题及日期容器的 li 节点
        for li in soup.find_all('li'):
            onclick_raw = li.get('onclick', '') or li.get('onClick', '')
            if onclick_raw and 'window.location.href' in str(onclick_raw):
                p_tag = li.find('p')
                a_tag = li.find('a')
                if p_tag and a_tag:
                    cust_items.append(li)

        if cust_items:
            current = []
            base_tag = soup.find('base', href=True)
            local_base_url = base_tag['href'].strip() if base_tag else base_url

            for li in cust_items:
                # 1. 提取标题（在 p 标签内）
                p_tag = li.find('p')
                title = p_tag.get_text(strip=True) if p_tag else ""
                title = re.sub(r'[…\s\.]+$', '', title).strip()

                if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                    continue

                # 2. 提取链接：从 onClick 属性中反向解码并匹配出真实地址
                url = ""
                onclick = li.get('onclick', '') or li.get('onClick', '')
                if onclick:
                    # 强力恢复 HTML 实体字符（如将 &#39; 解码恢复为单引号）
                    import html
                    onclick_decoded = html.unescape(str(onclick))
                    match = re.search(r"window\.location\.href\s*=\s*['\"](.*?)['\"]", onclick_decoded)
                    if match:
                        url = urljoin(local_base_url, match.group(1).strip())

                if not url:
                    continue

                # 3. 日期提取：直接读取无属性的空 a 标签文本，如 "2026-06-02"
                date_text = ""
                a_tag = li.find('a')
                if a_tag:
                    date_text = a_tag.get_text(strip=True)

                if not date_text:
                    date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self._recalc_current_dates(current)
                self.log.emit(f"[手工规则] 长春理工 li[onclick] 覆写卡片策略定位命中 → {len(current)} 条")
                return current

        # -------------------- 规则：陕师大等 a.item / date1 + date2 拆分自关联卡片 (纯 Python 无 LI 依赖版) --------------------
        snnu_items = []
        # 直接检索全页所有 class 属性包含 'item' 的 a 标签，彻底不依赖任何外层 li 或 div 容器
        for a in soup.find_all('a'):
            a_classes = a.get('class', [])
            if a_classes and any('item' in str(c).lower() for c in a_classes):
                # 校验核心特征：a 标签内必须同时包含 date1 和 date2 容器
                if a.select_one('.date1') and a.select_one('.date2'):
                    snnu_items.append(a)

        if snnu_items:
            current = []
            base_tag = soup.find('base', href=True)
            local_base_url = base_tag['href'].strip() if base_tag else base_url

            for a in snnu_items:
                # 1. 标题提取：优先获取 .title 容器的文字防止被 title 属性截断，其次读取 a 的 title 属性
                title_div = a.select_one('.text .title') or a.select_one('.title')
                title = title_div.get_text(strip=True) if title_div else a.get('title', '').strip()
                title = re.sub(r'[…\s\.]+$', '', title).strip()

                if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(local_base_url, a.get('href', '').strip())

                # 2. 日期提取：重组 date 内部的 .date2 (年-月) 和 .date1 (天，并对天补零) -> YYYY-MM-DD
                date_text = ""
                date_div = a.select_one('.date')
                if date_div:
                    d1 = date_div.select_one('.date1')
                    d2 = date_div.select_one('.date2')
                    if d1 and d2:
                        year_month = d2.get_text(strip=True).replace('/', '-')
                        day = d1.get_text(strip=True)
                        if year_month and day:
                            date_text = f"{year_month}-{day.zfill(2)}"

                if not date_text:
                    date_text = self._extract_date_from_element(a)

                current.append({"title": title, "url": url, "date": date_text})

            # 【系统级优化】：针对高特异性手工规则，将准入返回门槛由 >= 3 调整为 >= 1
            # 从而完美兼容尾页仅剩 1~2 条数据的极端情况，从根本上拦截因自动提取降级引发的探测噪音
            if len(current) >= 1:
                self._recalc_current_dates(current)
                self.log.emit(f"[手工规则] 陕师大 a.item (零LI依赖) 策略定位命中 → {len(current)} 条")
                return current

        # -------------------- 规则：长安大学等 .timg-linfo / .time1 复合列表 (纯 Python 零容器依赖版) --------------------
        changan_items = []
        # 直接在全页检索符合独立特征的行级节点，完全不依赖外层 .text-list 容器是否存在
        for li in soup.find_all('li'):
            li_id = li.get('id', '')
            if li_id and str(li_id).startswith('line_u'):
                # 检查是否包含核心卡片容器和日期标签
                timg_div = li.select_one('.timg-linfo') or li.find('div',
                                                                   class_=lambda
                                                                       c: c and 'timg-linfo' in str(
                                                                       c))
                time_span = li.select_one('.time1') or li.find('span',
                                                               class_=lambda c: c and 'time1' in str(c))
                if timg_div and time_span:
                    changan_items.append(li)

        if changan_items:
            current = []
            base_tag = soup.find('base', href=True)
            local_base_url = base_tag['href'].strip() if base_tag else base_url

            for li in changan_items:
                a = li.find('a', href=True)
                if not a:
                    continue

                # 1. 日期提取：重组 span.time1 内部的 b (天) 与剩余文本 (年-月) -> YYYY-MM-DD
                date_text = ""
                span_time = a.select_one('.time1') or a.find('span',
                                                             class_=lambda c: c and 'time1' in str(c))
                if span_time:
                    b_day = span_time.select_one('b') or span_time.find('b')
                    full_span_text = span_time.get_text(strip=True)  # 连读文本如 "03/2026-04"
                    if b_day:
                        day = b_day.get_text(strip=True)
                        # 清洗连读文本，剥除天数、斜杠和点号，提纯得到年-月
                        year_month = full_span_text.replace(day, "").replace('/', '').replace('.',
                                                                                              '-').strip()
                        if day and year_month:
                            date_text = f"{year_month}-{day.zfill(2)}"

                # 2. 标题提取与降噪：复制 h3 并对其内部的 .time1 标签进行物理剥离，防标题被日期污染
                h3 = a.select_one('.timg-linfo h3') or a.find('h3')
                if h3:
                    h3_copy = BeautifulSoup(str(h3), 'html.parser').find('h3')
                    time1_copy = h3_copy.select_one('.time1') or h3_copy.find('span', class_=lambda
                        c: c and 'time1' in str(c))
                    if time1_copy:
                        time1_copy.decompose()  # 移除复制出的日期节点
                    title = h3_copy.get_text(strip=True)
                else:
                    title = a.get_text(strip=True)
                    if date_text:
                        title = title.replace(date_text, "").strip()

                title = re.sub(r'[…\s\.]+$', '', title).strip()

                if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                    continue

                # 3. 智能拼接相对路径（自动适应 info/ 或 ../info/ 等多级相对跳转）
                url = urljoin(local_base_url, a.get('href', '').strip())

                if not date_text:
                    date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self._recalc_current_dates(current)
                self.log.emit(
                    f"[手工规则] 长安大学 .timg-linfo (零容器依赖) 策略定位命中 → {len(current)} 条")
                return current

        # -------------------- 规则：重师等 .subpage-list-item 自关联卡片结构 --------------------
        cqnu_list_items = soup.select('a.subpage-list-item, .subpage-list-wrap a')
        if cqnu_list_items:
            # 校验首个元素特征，本身是 a 标签且含有 p 标题与 span 日期
            first_item = cqnu_list_items[0]
            if first_item.select_one('p') and first_item.select_one('span'):
                current = []

                # 安全基准地址
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for a in cqnu_list_items:
                    if a.name != 'a' or not a.get('href'):
                        continue

                    # 提取标题
                    p_title = a.select_one('p')
                    title = p_title.get_text(strip=True) if p_title else a.get('title', '').strip()
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：读取 span 容器内的标准日期文本，如 "2025-03-21"
                    date_text = ""
                    span_date = a.select_one('span')
                    if span_date:
                        date_text = span_date.get_text(strip=True)

                    if not date_text:
                        date_text = self._extract_date_from_element(a)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 重师 .subpage-list-item 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：钢研总院等 .time > p + p 拆分日期卡片结构 --------------------
        cisri_items = soup.select('ul.clearfix li, .clearfix li')
        if not cisri_items:
            # 兜底：精确筛选含有 .time 且 .time 下包含至少两个 p 标签的所有 li 节点
            cisri_items = [li for li in soup.find_all('li') if
                           li.select_one('.time') and len(li.select_one('.time').find_all('p')) >= 2]

        if cisri_items:
            # 校验首个元素特征，本身含有 .time p 且含有 a 标签
            first_item = cisri_items[0]
            if first_item.select_one('.time p') and first_item.find('a', href=True):
                current = []

                # 安全基准地址
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for li in cisri_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签自带的 title 属性，防止文字被前端截断
                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：重组 time 内部的两个 p 标签 (第一个为天数，第二个为年-月) -> YYYY-MM-DD
                    date_text = ""
                    time_div = li.select_one('.time')
                    if time_div:
                        p_tags = time_div.find_all('p')
                        if len(p_tags) >= 2:
                            day = p_tags[0].get_text(strip=True)
                            year_month = p_tags[1].get_text(strip=True).replace('/', '-')
                            if day and year_month:
                                date_text = f"{year_month}-{day.zfill(2)}"

                    # 兜底
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 钢研总院 .time (双p) 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：贵州师大等 .list-listbox / 双 A 标签 + 裸露日期文本结构 --------------------
        # 此类结构下，首个 a 通常是分类链接 (如 [通知公告])，第二个才是真实公告；日期裸露在 li 文本中
        gznu_items = soup.select('ul.list-listbox li, .list-listbox li')
        if not gznu_items:
            # 兜底：精确寻找 class 包含 list-group-item 且包含双 A 标签和中括号的 li 节点
            gznu_items = [li for li in soup.find_all('li') if
                          'list-group-item' in li.get('class', []) and len(li.find_all('a', href=True)) >= 2]

        if gznu_items:
            # 校验首个元素特征，本身是 li 且包含至少两个 a 标签 [1]
            first_item = gznu_items[0]
            a_tags = first_item.find_all('a', href=True)
            if len(a_tags) >= 2:
                current = []
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for li in gznu_items:
                    all_a = li.find_all('a', href=True)
                    if len(all_a) < 2:
                        valid_a = all_a[0] if all_a else None
                    else:
                        # 核心防线：跳过可能存在的分类导航 A 标签，精准读取第二个详情 A 标签
                        valid_a = None
                        for a in all_a:
                            a_text = a.get_text(strip=True)
                            # 如果是类似 [通知公告] 等分类前缀，视为噪音并过滤
                            if a_text.startswith('[') and a_text.endswith(']'):
                                continue
                            valid_a = a
                            break

                        if not valid_a:
                            valid_a = all_a[1]  # 默认读取第二个

                    if not valid_a:
                        continue

                    # 提取标题
                    title = valid_a.get('title', '').strip()
                    if not title:
                        title = valid_a.get_text(strip=True)
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, valid_a.get('href', '').strip())

                    # 日期提取：由于日期无独立标签容器，直接从 li 的完整 text 中用正则提取
                    date_text = ""
                    li_text = li.get_text(" ", strip=True)
                    # 优先匹配 YYYY年MM月DD日
                    m_date = re.search(r'(\d{4}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*日)', li_text)
                    if m_date:
                        date_text = m_date.group(1)
                    else:
                        # 备用匹配 YYYY-MM-DD
                        m_date_std = re.search(r'(\d{4}[-./]\d{1,2}[-./]\d{1,2})', li_text)
                        if m_date_std:
                            date_text = m_date_std.group(1)

                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(
                        f"[手工规则] 贵州师大 .list-listbox (双A标签) 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：重庆理工等 .list-text / tm > p + span 拆分日期卡片结构 --------------------
        cqut_list_items = soup.select('div.list-text li, .list-text li')
        if not cqut_list_items:
            # 兜底：筛选所有含有 .tm 且含有 .text h1 的 li 节点
            cqut_list_items = [li for li in soup.find_all('li') if li.select_one('.tm') and li.select_one('.text h1')]

        if cqut_list_items:
            # 校验首个元素特征，必须含有 .tm p 且有 .tm span，以及 .text h1
            first_item = cqut_list_items[0]
            if first_item.select_one('.tm p') and first_item.select_one('.tm span') and (
                    first_item.select_one('.text h1') or first_item.select_one('h1')):
                current = []
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for li in cqut_list_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题
                    h1 = a.select_one('.text h1') or a.select_one('h1')
                    title = h1.get_text(strip=True) if h1 else a.get('title', '').strip()
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：重组 tm 内部的 span (年份) 和 p (月-日) -> YYYY-MM-DD
                    date_text = ""
                    tm_div = a.select_one('.tm')
                    if tm_div:
                        span = tm_div.select_one('span')
                        p_day = tm_div.select_one('p')
                        if span and p_day:
                            year = span.get_text(strip=True)
                            month_day = p_day.get_text(strip=True).replace('/', '-')
                            if year and month_day:
                                date_text = f"{year}-{month_day}"

                    # 兜底
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 重庆理工 .list-text 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：西湖大学等脚本 JSON 数据嵌入格式 (allData 嵌入式) --------------------
        script_tags = soup.find_all('script')
        for script in script_tags:
            script_text = script.get_text()
            if 'allData' in script_text or 'new graduate' in script_text:
                # 提取所有不含嵌套大括号的完整 { ... } 属性块
                blocks = re.findall(r'\{[^{}]*?\}', script_text, re.DOTALL)
                current = []
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for block in blocks:
                    # 校验该特征块必须同时包含 url 和 date 属性（兼容带/不带引号及空格的键名）
                    if not (re.search(r'["\']?url["\']?\s*:', block) and re.search(r'["\']?date["\']?\s*:',
                                                                                   block)):
                        continue

                    # 1. 提取链接
                    url_match = re.search(r'["\']?url["\']?\s*:\s*["\'](.*?)["\']', block)
                    if not url_match:
                        continue
                    url_raw = url_match.group(1).strip()
                    if not url_raw or url_raw.startswith(('javascript:', '#', 'mailto:')):
                        continue
                    url = urljoin(local_base_url, url_raw)

                    # 2. 提取日期
                    date_match = re.search(r'["\']?date["\']?\s*:\s*["\'](.*?)["\']', block)
                    if not date_match:
                        continue
                    date_text = date_match.group(1).strip()

                    # 3. 提取标题 (兼容宣讲信息里的 title 字段和普通列表里的 text 字段)
                    title_match = re.search(r'["\']?(text|title)["\']?\s*:\s*["\'](.*?)["\']', block)
                    if not title_match:
                        continue
                    title = title_match.group(2).strip()
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    # 重新计算并格式化相对天数
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 脚本 JSON 嵌入式列表 (allData) 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：西外等 .tzgg / span.flex > i 拆分日期卡片结构 --------------------
        xisu_items = soup.select('.tzgg li, div.tzgg li')
        if not xisu_items:
            # 兜底：利用 Python 循环精准筛选符合特征的 li 节点
            xisu_items = [li for li in soup.find_all('li') if
                          li.find('a', class_='wl') and li.select_one('span.flex i')]

        if xisu_items:
            # 校验首个元素特征，必须含有 a 且 a 内部有 span.flex i 标签和 h3 标题
            first_item = xisu_items[0]
            first_a = first_item.find('a')
            if first_a and first_a.select_one('span.flex i') and first_a.select_one('h3.txt1'):
                current = []

                # 安全基准地址
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for li in xisu_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题
                    h3 = a.select_one('h3.txt1') or a.select_one('h3')
                    title = h3.get_text(strip=True) if h3 else a.get('title', '').strip()
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：重组 span.flex 内部的 i (天) 与外部文本 (年-月) -> YYYY-MM-DD
                    date_text = ""
                    span_date = a.select_one('span.flex')
                    if span_date:
                        i_tag = span_date.select_one('i')
                        full_span_text = span_date.get_text(strip=True)  # 连读文本如 "02/2024-12"
                        if i_tag:
                            day = i_tag.get_text(strip=True)
                            # 从连读文本中刨除天数与斜杠
                            year_month = full_span_text.replace(day, "").replace('/', '').strip()
                            if day and year_month:
                                date_text = f"{year_month}-{day.zfill(2)}"

                    # 兜底
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 西外 .tzgg (span.flex) 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：西湖大学等 .new_every / onclick window.open 复合卡片结构 --------------------
        new_every_items = soup.select('.news_content .new_every, .new_every')
        if not new_every_items:
            # 兜底：精准筛选含有 news_desc 且 news_desc 下含有 onclick window.open 的所有 div 节点
            new_every_items = [div for div in soup.find_all('div', class_='new_every') if
                               div.select_one('.news_desc') and div.select_one('.news_desc').get('onclick')]

        if new_every_items:
            # 校验首个元素特征，本身含有 .news_desc 且含有 .news_time 容器
            first_item = new_every_items[0]
            if first_item.select_one('.news_desc') and first_item.select_one('.news_time'):
                current = []

                # 安全基准地址
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for div in new_every_items:
                    desc_p = div.select_one('.news_desc')
                    if not desc_p:
                        continue

                    # 提取标题
                    title = desc_p.get_text(strip=True)
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    # 提取链接：由于没有标准的 <a> 标签，我们从 onclick 属性中用正则匹配出真正的地址
                    url = ""
                    onclick = desc_p.get('onclick', '')
                    if onclick:
                        match = re.search(r"window\.open\(\s*['\"](.*?)['\"]", onclick)
                        if match:
                            url = urljoin(local_base_url, match.group(1).strip())

                    if not url:
                        continue

                    # 日期提取：读取 .news_time 下包含四位年份数字的 span，或者读取整行文本
                    date_text = ""
                    time_p = div.select_one('.news_time')
                    if time_p:
                        for span in time_p.find_all('span'):
                            span_text = span.get_text(strip=True)
                            if re.search(r'\d{4}', span_text):
                                date_text = span_text
                                break
                        if not date_text:
                            date_text = time_p.get_text(strip=True)

                    if not date_text:
                        date_text = self._extract_date_from_element(div)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 通用 .new_every (window.open) 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：西安交大等 .ltrlb_tit 模板通用匹配 (树重建容错级) --------------------
        xjtu_marx_items = []
        for li in soup.find_all('li'):
            li_id = li.get('id', '')
            if li_id and str(li_id).startswith('line_u'):
                # 绕过 h2 标签的非法嵌套纠错干扰，直接对整个 li 节点检索包含 'ltrlb_tit' 类名的标题 div
                title_div = li.select_one('.ltrlb_tit') or li.find('div',
                                                                   class_=lambda c: c and 'ltrlb_tit' in str(c))
                if title_div:
                    xjtu_marx_items.append(li)

        if xjtu_marx_items:
            current = []
            base_tag = soup.find('base', href=True)
            local_base_url = base_tag['href'].strip() if base_tag else base_url

            for li in xjtu_marx_items:
                a = li.find('a', href=True)
                if not a:
                    continue

                # 1. 标题提取：优先直接提取已定位的标题 div 文本；其次读取 a 的 title 属性兜底
                title_div = li.select_one('.ltrlb_tit') or li.find('div',
                                                                   class_=lambda c: c and 'ltrlb_tit' in str(c))
                title = title_div.get_text(strip=True) if title_div else a.get('title', '').strip()
                title = re.sub(r'[…\s\.]+$', '', title).strip()

                if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(local_base_url, a.get('href', '').strip())

                # 2. 日期提取：检索整个 li 节点下任何满足 YYYY-MM-DD 的 span，彻底免疫 h2 闭合干扰
                date_text = ""
                for span in li.find_all('span'):
                    span_text = span.get_text(strip=True)
                    if re.match(r'^\d{4}[-.]\d{1,2}[-.]\d{1,2}$', span_text):
                        date_text = span_text
                        break

                if not date_text:
                    date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self._recalc_current_dates(current)
                self.log.emit(f"[手工规则] 西安交大 .ltrlb_tit (DOM容错) 策略定位命中 → {len(current)} 条")
                return current

        # -------------------- 规则：西安交大等 .mainCon (纯 Python 容错级遍历匹配) --------------------
        main_con_ul = None
        # 兼容大小写 UL/ul，检索 class 属性中包含 maincon 的标签
        for ul in soup.find_all(['ul', 'UL']):
            ul_classes = ul.get('class', [])
            if ul_classes and any('maincon' in str(c).lower() for c in ul_classes):
                main_con_ul = ul
                break

        if main_con_ul:
            current = []
            base_tag = soup.find('base', href=True)
            local_base_url = base_tag['href'].strip() if base_tag else base_url

            # 遍历 li 节点
            li_nodes = main_con_ul.find_all('li')
            for li in li_nodes:
                a = li.find('a', href=True)
                if not a:
                    continue

                # 1. 链接提取
                url = urljoin(local_base_url, a.get('href', '').strip())

                # 2. 标题提取：优先读取 a 的 title；其次寻找 class 包含 'lf' 的 span；最后降级读取整个 a 文本
                title = a.get('title', '').strip()
                if not title:
                    span_lf = None
                    for span in a.find_all('span'):
                        span_classes = span.get('class', [])
                        if span_classes and any('lf' in str(c).lower() for c in span_classes):
                            span_lf = span
                            break
                    if span_lf:
                        title = span_lf.get_text(strip=True)
                    else:
                        title = a.get_text(strip=True)

                title = re.sub(r'[…\s\.]+$', '', title).strip()

                if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                    continue

                # 3. 日期提取：寻找 class 包含 'rt' 的 b 标签，若无则使用首个 b 标签兜底
                date_text = ""
                b_rt = None
                for b in a.find_all('b'):
                    b_classes = b.get('class', [])
                    if b_classes and any('rt' in str(c).lower() for c in b_classes):
                        b_rt = b
                        break

                if b_rt:
                    date_text = b_rt.get_text(strip=True)
                else:
                    first_b = a.find('b')
                    if first_b:
                        date_text = first_b.get_text(strip=True)

                if not date_text:
                    date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self._recalc_current_dates(current)
                self.log.emit(f"[手工规则] 西安交大 .mainCon (容错遍历) 策略定位命中 → {len(current)} 条")
                return current

        # -------------------- 规则：西财等 .m-listb2 / span.s1 + span.s2 拆分日期卡片结构 --------------------
        swufe_items = soup.select('ul.m-listb2 li, .m-listb2 li')
        if swufe_items:
            # 校验首个元素特征，本身含有 .time .s1 与 .time .s2，且有 h3 标题
            first_item = swufe_items[0]
            if first_item.select_one('.time .s1') and first_item.select_one('.time .s2') and (
                    first_item.select_one('h3') or first_item.select_one('.txt h3')):
                current = []

                # 安全基准地址
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for li in swufe_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签自带的 title 属性，防止文字被前端截断
                    title = a.get('title', '').strip()
                    if not title:
                        h3 = a.select_one('h3')
                        title = h3.get_text(strip=True) if h3 else a.get_text(strip=True)
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：重组 time 内部的 span.s2 (年.月，并将点号替换为连字符) 和 span.s1 (天，并对天补零) -> YYYY-MM-DD
                    date_text = ""
                    time_div = a.select_one('.time')
                    if time_div:
                        s1 = time_div.select_one('.s1')
                        s2 = time_div.select_one('.s2')
                        if s1 and s2:
                            year_month = s2.get_text(strip=True).replace('.', '-')
                            day = s1.get_text(strip=True)
                            if year_month and day:
                                date_text = f"{year_month}-{day.zfill(2)}"

                    # 兜底
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 西财 .m-listb2 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：通用 .text-list / text-ldata + text-linfo 复合卡片结构 --------------------
        text_list_items = soup.select('.text-list li, div.text-list li')
        if not text_list_items:
            # 兜底：筛选所有含有 .text-ldata 且含有 .text-linfo 容器的 li 节点
            text_list_items = [li for li in soup.find_all('li') if
                               li.select_one('.text-ldata') and li.select_one('.text-linfo')]

        if text_list_items:
            # 校验首个元素特征，必须含有 .text-ldata 容器与 h3 标题
            first_item = text_list_items[0]
            if first_item.select_one('.text-ldata') and (
                    first_item.select_one('.text-linfo h3') or first_item.select_one('h3')):
                current = []

                # 安全基准地址
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for li in text_list_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签自带的 title 属性，防止文字被前端截断
                    title = a.get('title', '').strip()
                    if not title:
                        h3 = a.select_one('.text-linfo h3') or a.select_one('h3')
                        title = h3.get_text(strip=True) if h3 else a.get_text(strip=True)
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：重组 text-ldata 内部的 span (年份) 和 p (月/日，并对斜杠进行连字符替换) -> YYYY-MM-DD
                    date_text = ""
                    ldata_div = a.select_one('.text-ldata')
                    if ldata_div:
                        span = ldata_div.select_one('span')
                        p_day = ldata_div.select_one('p')
                        if span and p_day:
                            year = span.get_text(strip=True)
                            month_day = p_day.get_text(strip=True).replace('/', '-')
                            if year and month_day:
                                date_text = f"{year}-{month_day}"

                    # 兜底
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 通用 .text-list / text-ldata 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：西南大学 .con_zt_cen_nero_lb / strong + span 拆分日期卡片结构 --------------------
        swu_zt_items = soup.select('ul.con_zt_cen_nero_lb li, .con_zt_cen_nero_lb li')
        if swu_zt_items:
            # 校验首个元素特征，必须含有 .con_zt_cen_nero_lb_rl 容器内的 strong 和 span，且有标题 h3
            first_item = swu_zt_items[0]
            if first_item.select_one('.con_zt_cen_nero_lb_rl strong') and first_item.select_one(
                    '.con_zt_cen_nero_lb_rl span') and first_item.select_one('h3'):
                current = []

                # 安全基准地址
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for li in swu_zt_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题
                    h3 = a.select_one('h3')
                    title = h3.get_text(strip=True) if h3 else a.get('title', '').strip()
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：重组 con_zt_cen_nero_lb_rl 内部的 span (年-月) 和 strong (天，并对天补零) -> YYYY-MM-DD
                    date_text = ""
                    rl_div = a.select_one('.con_zt_cen_nero_lb_rl')
                    if rl_div:
                        strong = rl_div.select_one('strong')
                        span = rl_div.select_one('span')
                        if strong and span:
                            year_month = span.get_text(strip=True).replace('/', '-')
                            day = strong.get_text(strip=True)
                            if year_month and day:
                                date_text = f"{year_month}-{day.zfill(2)}"

                    # 兜底
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 西南大学 .con_zt_cen_nero_lb 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：西南大学等 .list / tim > span + .day 拆分日期卡片结构 --------------------
        swu_list_items = soup.select('div.list a, .list a')
        if swu_list_items:
            # 校验首个元素特征，本身是 a 标签且含有 .tim .day 与 .tim span，并有标题标签 p
            first_item = swu_list_items[0]
            if first_item.select_one('.tim .day') and first_item.select_one(
                    '.tim span') and first_item.select_one('p'):
                current = []

                # 安全基准地址
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for a in swu_list_items:
                    if a.name != 'a' or not a.get('href'):
                        continue

                    # 提取标题
                    p_title = a.select_one('p')
                    title = p_title.get_text(strip=True) if p_title else a.get('title', '').strip()
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：重组 tim 内部的 span (年-月) 和 .day (日，并对日补零) -> YYYY-MM-DD
                    date_text = ""
                    tim_div = a.select_one('.tim')
                    if tim_div:
                        span = tim_div.select_one('span')
                        day_div = tim_div.select_one('.day')
                        if span and day_div:
                            year_month = span.get_text(strip=True).replace('/', '-')
                            day = day_div.get_text(strip=True)
                            if year_month and day:
                                date_text = f"{year_month}-{day.zfill(2)}"

                    # 兜底
                    if not date_text:
                        date_text = self._extract_date_from_element(a)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 西南大学 .list 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：西南医科大等 .article-list / h3 a + p.info 复合列表结构 --------------------
        swmu_items = soup.select('.article-list li, div.article-list li')
        if swmu_items:
            # 校验首个元素特征，必须含有 h3 a 且含有 p.info 属性
            first_item = swmu_items[0]
            if first_item.select_one('h3 a') and first_item.select_one('p.info'):
                current = []

                # 安全基准地址
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for li in swmu_items:
                    a = li.select_one('h3 a') or li.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题
                    title = a.get_text(strip=True)
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：读取 p.info 内的时间并剥离分享相关的子标签噪声
                    date_text = ""
                    info_p = li.select_one('p.info')
                    if info_p:
                        # 制作临时副本并剔除 .share 分享容器，防止污染原 DOM 树
                        info_copy = BeautifulSoup(str(info_p), 'html.parser').select_one('p')
                        share_tag = info_copy.select_one('.share')
                        if share_tag:
                            share_tag.decompose()

                        raw_info = info_copy.get_text(strip=True)
                        # 利用正则提取标准发布时间
                        m_date = re.search(r'(\d{4}-\d{1,2}-\d{1,2})', raw_info)
                        if m_date:
                            date_text = m_date.group(1)
                        else:
                            date_text = raw_info

                    # 兜底
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 西南医科大 .article-list 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：西南交大等 .newsCont / time b + em 拆分日期卡片结构 --------------------
        news_cont_items = soup.select('.newsCont li, div.newsCont li')
        if news_cont_items:
            # 校验首个元素特征，必须含有 .time b 与 .time em，且有 h5 标题
            first_item = news_cont_items[0]
            if first_item.select_one('.time b') and first_item.select_one('.time em') and (
                    first_item.select_one('.text h5') or first_item.select_one('h5')):
                current = []

                # 安全基准地址
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for li in news_cont_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签自带的 title 属性，防止文字被前端截断
                    title = a.get('title', '').strip()
                    if not title:
                        h5 = a.select_one('.text h5') or a.select_one('h5')
                        title = h5.get_text(strip=True) if h5 else a.get_text(strip=True)
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：重组 time 内部的 em (年-月) 和 b (日，并对日补零) -> YYYY-MM-DD
                    date_text = ""
                    time_div = a.select_one('.time')
                    if time_div:
                        b_tag = time_div.select_one('b')
                        em_tag = time_div.select_one('em')
                        if b_tag and em_tag:
                            day = b_tag.get_text(strip=True)
                            year_month = em_tag.get_text(strip=True).replace('/', '-')
                            if day and year_month:
                                date_text = f"{year_month}-{day.zfill(2)}"

                    # 兜底
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 西南交大 .newsCont 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：西工大等 .tlist / date span + b 拆分日期卡片结构 --------------------
        tlist_items = soup.select('.tlist li, div.tlist li')
        if tlist_items:
            # 校验首个元素特征，必须含有 .date span 与 .date b，且有 h3 标题
            first_item = tlist_items[0]
            if first_item.select_one('.date span') and first_item.select_one('.date b') and (
                    first_item.select_one('.info h3') or first_item.select_one('h3')):
                current = []

                # 安全基准地址
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for li in tlist_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签自带的 title 属性，防止文字被前端截断
                    title = a.get('title', '').strip()
                    if not title:
                        h3 = a.select_one('.info h3') or a.select_one('h3')
                        title = h3.get_text(strip=True) if h3 else a.get_text(strip=True)
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：重组 date 内部的 span (年份) 和 b (月/日，并将斜杠替换为连字符) -> YYYY-MM-DD
                    date_text = ""
                    date_div = a.select_one('.date')
                    if date_div:
                        span = date_div.select_one('span')
                        b_day = date_div.select_one('b')
                        if span and b_day:
                            year = span.get_text(strip=True)
                            month_day = b_day.get_text(strip=True).replace('/', '-')
                            if year and month_day:
                                date_text = f"{year}-{month_day}"

                    # 兜底
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 西工大 .tlist 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：福建中医药大等 .newslist / a > span 复合卡片结构 --------------------
        fj_news_items = soup.select('.newslist a, div.newslist a')
        if fj_news_items:
            # 校验首个元素特征，本身是 a 标签且含有 span 容器（内含 4 位数字年份）
            first_item = fj_news_items[0]
            if first_item.select_one('span') and re.search(r'\d{4}', first_item.select_one('span').get_text()):
                current = []
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for a in fj_news_items:
                    if a.name != 'a' or not a.get('href'):
                        continue

                    # 提取日期：从内部的 span 提取，如 "2026.05.19"
                    date_text = ""
                    span_date = a.select_one('span')
                    if span_date:
                        date_text = span_date.get_text(strip=True)

                    # 提取并降噪标题：利用父子元素文本抠除法，把 a 标签完整文本中干净去除 span 的时间
                    title = a.get_text(strip=True)
                    if date_text:
                        title = title.replace(date_text, "").strip()

                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    # 过滤导航或空文本
                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    if not date_text:
                        date_text = self._extract_date_from_element(a)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 福建中医药大 .newslist 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：甘肃农大等 .page-content-body / a > span + em 复合列表结构 --------------------
        gsau_items = soup.select('.page-content-body li, div.page-content-body li')
        if not gsau_items:
            # 兜底：利用 Python 循环精准筛选所有在 a 标签内部同时包含 span 与 em 节点的 li
            gsau_items = [li for li in soup.find_all('li') if
                          li.find('a') and li.find('a').find('span') and li.find('a').find('em')]

        if gsau_items:
            # 校验首个元素特征，必须含有 a 标签且 a 内部有 span 和 em 容器
            first_item = gsau_items[0]
            first_a = first_item.find('a')
            if first_a and first_a.select_one('span') and first_a.select_one('em'):
                current = []

                # 安全基准地址
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for li in gsau_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题（在 a 内部的 span 容器内）
                    span_title = a.select_one('span')
                    title = span_title.get_text(strip=True) if span_title else a.get_text(strip=True)
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：直接读取 a 内部的 em 标签，如 "2026-04-17"
                    date_text = ""
                    em_date = a.select_one('em')
                    if em_date:
                        date_text = em_date.get_text(strip=True)

                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 甘肃农大 .page-content-body 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：Joomla 系统 com_content 分类列表表格结构 (cat-list-row) --------------------
        joomla_rows = soup.select('tr[class*="cat-list-row"], tr:has(.list-title):has(.list-date)')
        if not joomla_rows:
            # 兜底：通过 Python 循环精准匹配同时含有 .list-title 和 .list-date 容器的 tr 节点
            joomla_rows = [tr for tr in soup.find_all('tr') if
                           tr.select_one('.list-title') and tr.select_one('.list-date')]

        if joomla_rows:
            # 校验首个元素特征，必须在 .list-title 内部包含 a 标签且含有 .list-date 容器
            first_item = joomla_rows[0]
            if first_item.select_one('.list-title a') and first_item.select_one('.list-date'):
                current = []

                # 安全基准地址
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for tr in joomla_rows:
                    a = tr.select_one('.list-title a') or tr.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题
                    title = a.get_text(strip=True)
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：直接读取 .list-date，如 "2026年5月29日"
                    date_text = ""
                    date_td = tr.select_one('.list-date')
                    if date_td:
                        date_text = date_td.get_text(strip=True)

                    if not date_text:
                        date_text = self._extract_date_from_element(tr)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] Joomla 分类列表表格策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：湖师大等 .notice-item 空 A 标签覆盖式卡片结构 --------------------
        hunnu_items = soup.select('.notice-list .notice-item, .notice-item')
        if hunnu_items:
            # 校验首个元素特征，必须含有标题容器 .ni-title 且含有日期容器 .ni-date
            first_item = hunnu_items[0]
            if first_item.select_one('.ni-title') and first_item.select_one('.ni-date'):
                current = []

                # 安全基准地址
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for div in hunnu_items:
                    a = div.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题（由于 A 标签为空，必须从同级 .ni-title 容器中提取标题）
                    title_div = div.select_one('.ni-title')
                    title = title_div.get_text(strip=True) if title_div else a.get('title', '').strip()
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：读取 .ni-date 容器内的文本，如 "2026-04-07"
                    date_text = ""
                    date_div = div.select_one('.ni-date')
                    if date_div:
                        date_text = date_div.get_text(strip=True)

                    if not date_text:
                        date_text = self._extract_date_from_element(div)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 湖师大 .notice-item 覆盖卡片策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：湖大新传等 a.list-item 自关联卡片结构 --------------------
        hnu_list_items = soup.select('a.list-item, .content-list a')
        if hnu_list_items:
            # 校验首个元素特征，本身是 a 标签且含有 .item-date 容器与 h3 标题
            first_item = hnu_list_items[0]
            if first_item.select_one('.item-date') and (
                    first_item.select_one('h3') or first_item.select_one('.item-content h3')):
                current = []

                # 安全基准地址
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for a in hnu_list_items:
                    if a.name != 'a' or not a.get('href'):
                        continue

                    # 提取标题
                    h3 = a.select_one('h3')
                    title = h3.get_text(strip=True) if h3 else a.get('title', '').strip()
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：重组 span.date (年-月，如 2026-05) 与 span.day (天，如 19) -> YYYY-MM-DD
                    date_text = ""
                    date_div = a.select_one('.item-date')
                    if date_div:
                        span_date = date_div.select_one('.date')
                        span_day = date_div.select_one('.day')
                        if span_date and span_day:
                            ym = span_date.get_text(strip=True).replace('/', '-')
                            d = span_day.get_text(strip=True)
                            if ym and d:
                                date_text = f"{ym}-{d.zfill(2)}"

                    if not date_text:
                        date_text = self._extract_date_from_element(a)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 湖大新传 a.list-item 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：通用 .new-list-item 响应式列表卡片结构 --------------------
        new_list_items = soup.select('a.new-list-item, .new-list-item')
        if new_list_items:
            # 校验首个元素特征，必须含有 title/span 标签以及 small 日期标签
            first_item = new_list_items[0]
            if (first_item.select_one('.new-list-item-title') or first_item.select_one(
                    'span')) and first_item.select_one('small'):
                current = []

                # 安全基准地址
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for a in new_list_items:
                    if a.name != 'a' or not a.get('href'):
                        continue

                    # 提取标题并优雅移除内部的 SVG 节点（防止 SVG 标签中夹杂干扰文本）
                    span_title = a.select_one('.new-list-item-title span') or a.select_one('span')
                    if span_title:
                        # 制作临时副本并剔除 svg，防止污染原 DOM 树
                        span_copy = BeautifulSoup(str(span_title), 'html.parser').select_one('span')
                        svg_tag = span_copy.select_one('svg')
                        if svg_tag:
                            svg_tag.decompose()
                        title = span_copy.get_text(strip=True)
                    else:
                        title = a.get_text(strip=True)

                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：读取 small 标签内的标准日期文本，如 "2026-05-19"
                    date_text = ""
                    small_date = a.select_one('small')
                    if small_date:
                        date_text = small_date.get_text(strip=True)

                    if not date_text:
                        date_text = self._extract_date_from_element(a)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 通用 .new-list-item 策略定位命中 → {len(current)} 条")
                    return current

            # -------------------- 规则：湖南农大等 .right_list 非标自闭合 A 标签结构 --------------------
        rl_items = soup.select('.right_list li, div.right_list li')
        if rl_items:
            # 过滤掉样式中带有 'line' 样式的空行辅助分割线
            rl_items = [li for li in rl_items if 'line' not in li.get('class', [])]

            # 校验首个元素特征，必须含有 span 且包含 a 标签
            first_item = rl_items[0] if rl_items else None
            if first_item and first_item.select_one('span') and first_item.find('a', href=True):
                current = []

                # 安全基准地址
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for li in rl_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 日期提取：读取 span 容器
                    date_text = ""
                    span_date = li.select_one('span')
                    if span_date:
                        date_text = span_date.get_text(strip=True)

                    # 标题提取：防范自闭合 <a href="..."/> 导致 BeautifulSoup 获取文本为空的非标情况
                    title = a.get_text(strip=True)
                    if not title:
                        # 兜底防御：从父级 li 的完整文本中，完美抠除已提取的 span 日期文本，提纯出标题
                        li_text = li.get_text(strip=True)
                        if date_text:
                            title = li_text.replace(date_text, "").strip()
                        else:
                            title = li_text

                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 湖南农大 .right_list 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：温州医大等 .ej_list2 / p.bt + p.sj 复合卡片结构 --------------------
        ej_list_items = soup.select('.ej_list2 li, div.ej_list2 li')
        if ej_list_items:
            # 校验首个元素特征，必须含有标题 p.bt 且含有日期 p.sj
            first_item = ej_list_items[0]
            if first_item.select_one('p.bt') and first_item.select_one('p.sj'):
                current = []

                # 安全基准地址
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for li in ej_list_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题（优先提取 p.bt 容器）
                    bt_p = a.select_one('p.bt')
                    title = bt_p.get_text(strip=True) if bt_p else a.get('title', '').strip()
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：读取 p.sj 容器内的标准日期文本，如 "2026-05-11"
                    date_text = ""
                    sj_p = a.select_one('p.sj')
                    if sj_p:
                        date_text = sj_p.get_text(strip=True)

                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 温州医大 .ej_list2 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：清华医管院 span中文月份 + h3 标题结构 --------------------
        thu_items = soup.select('ul.list li[id^="line_u15_"]')
        if not thu_items:
            # 兜底：通过 Python 循环精准筛选含有中文月份样式 span 与 h3 标签的 li 节点
            thu_items = [li for li in soup.find_all('li') if
                         li.find('span') and li.find('h3') and any(m in li.find('span').get_text() for m in ['月'])]
        # -------------------- 规则：清华医管院 span中文月份 + h3 标题结构 --------------------
        if thu_items:
            # 校验首个元素特征，必须含有 span 且有 h3 标签
            first_item = thu_items[0]
            if first_item.select_one('span') and first_item.select_one('h3'):
                current = []

                # 安全基准地址
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for li in thu_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题
                    h3 = a.select_one('h3')
                    title = h3.get_text(strip=True) if h3 else a.get_text(strip=True)
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：读取 span 内包含中文月份的完整文本，将其传递给升级后的底层解析器
                    date_text = ""
                    span_date = a.select_one('span')
                    if span_date:
                        date_text = span_date.get_text(strip=True)

                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 清华医管院中文月份策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：深大等 a.news-item 自关联卡片结构 --------------------
        szu_news_items = soup.select('a.news-item, .news-list-wrap a')
        if szu_news_items:
            # 校验首个元素特征，本身是 a 标签且含有 .date 与 .title 容器
            first_item = szu_news_items[0]
            if first_item.select_one('.date') and first_item.select_one('.title'):
                current = []

                # 安全基准地址
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for a in szu_news_items:
                    if a.name != 'a' or not a.get('href'):
                        continue

                    # 提取标题
                    title_div = a.select_one('.title')
                    title = title_div.get_text(strip=True) if title_div else a.get('title', '').strip()
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：读取 .date 容器内的文本，如 "2026/04/21"
                    date_text = ""
                    date_div = a.select_one('.date')
                    if date_div:
                        date_text = date_div.get_text(strip=True)

                    if not date_text:
                        date_text = self._extract_date_from_element(a)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 深大 a.news-item 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：浙江海大 .list_item / div.title + a.more 复合卡片结构 --------------------
        zjou_items = soup.select('.list_item, div.list_item')
        if zjou_items:
            # 校验首个元素特征，必须含有 .date, .title 且有 a.more
            first_item = zjou_items[0]
            if first_item.select_one('.date') and first_item.select_one('.title') and first_item.select_one(
                    'a.more'):
                current = []

                # 安全基准地址
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for div in zjou_items:
                    a = div.select_one('a.more') or div.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题（优先提取 .title 容器）
                    title_div = div.select_one('.title')
                    title = title_div.get_text(strip=True) if title_div else a.get('title', '').strip()
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回', '查看详情']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：读取 .date 容器内的文本，如 "2026年05月12日"
                    date_text = ""
                    date_div = div.select_one('.date')
                    if date_div:
                        date_text = date_div.get_text(strip=True)

                    if not date_text:
                        date_text = self._extract_date_from_element(div)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 浙江海大 .list_item 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：河南工大 .ul-pagenews / day + year 拆分日期卡片结构 --------------------
        page_news_items = soup.select('ul.ul-pagenews li, .ul-pagenews li')
        if page_news_items:
            # 校验首个元素特征，必须含有 .day, .year 且有 .tit 标题
            first_item = page_news_items[0]
            if first_item.select_one('.day') and first_item.select_one('.year') and first_item.select_one(
                    '.tit'):
                current = []

                # 安全基准地址
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for li in page_news_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 优先提取 a 标签自带的 title 属性，防止文字被前端截断
                    title = a.get('title', '').strip()
                    if not title:
                        tit_div = a.select_one('.tit')
                        title = tit_div.get_text(strip=True) if tit_div else a.get_text(strip=True)
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：重组 year (年份) 和 day (月.日) 并将点号替换为连字符 -> YYYY-MM-DD
                    date_text = ""
                    year_div = a.select_one('.year')
                    day_div = a.select_one('.day')
                    if year_div and day_div:
                        year = year_div.get_text(strip=True)
                        month_day = day_div.get_text(strip=True).replace('.', '-')
                        if year and month_day:
                            date_text = f"{year}-{month_day}"

                    # 兜底
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 河南工大 .ul-pagenews 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：郑州工院 a.flex.wl / date span 拆分日期卡片结构 --------------------
        zzut_items = soup.select('li:has(a.flex.wl), li:has(a.wl), li[data-aos] a.wl')
        if not zzut_items:
            # 兜底：通过 Python 循环精准筛选符合特征的 li 行级元素
            zzut_items = [li for li in soup.find_all('li') if
                          li.find('a', class_=lambda c: c and 'wl' in c) and li.select_one('.date span')]

        if zzut_items:
            # 校验首个元素特征，必须含有 .date span 且有 h3 标题
            first_item = zzut_items[0]
            if first_item.select_one('.date span') and (
                    first_item.select_one('h3.txt1') or first_item.select_one('.con h3')):
                current = []

                # 安全基准地址
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for li in zzut_items:
                    a = li if li.name == 'a' else li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签自带的 title 属性，防止文字被前端截断
                    title = a.get('title', '').strip()
                    if not title:
                        h3 = a.select_one('h3')
                        title = h3.get_text(strip=True) if h3 else a.get_text(strip=True)
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：重组 date 内部的 span (年份) 和外部文字 (月-日) -> YYYY-MM-DD
                    date_text = ""
                    date_div = a.select_one('.date')
                    if date_div:
                        span = date_div.select_one('span')
                        full_text = date_div.get_text(strip=True)  # 获取连读文本，如 "202603-27"
                        if span:
                            year = span.get_text(strip=True)
                            # 从连读文本中刨除年份，提纯月日信息
                            month_day = full_text.replace(year, "", 1).replace('/', '-').strip()
                            if year and month_day:
                                date_text = f"{year}-{month_day}"

                    # 兜底
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 郑州工院 a.flex.wl 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：商丘师院 .fylb / yy + mmdd 拆分日期卡片结构 --------------------
        fylb_items = soup.select('ul.fylb li, .fylb li')
        if fylb_items:
            # 校验首个元素特征，必须含有 .yy, .mmdd 且有 h4 标题
            first_item = fylb_items[0]
            if first_item.select_one('.yy') and first_item.select_one('.mmdd') and first_item.select_one('h4'):
                current = []

                # 安全基准地址
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for li in fylb_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 优先提取 a 标签自带的 title 属性，防止文字被前端截断
                    title = a.get('title', '').strip()
                    if not title:
                        h4 = a.select_one('h4')
                        title = h4.get_text(strip=True) if h4 else a.get_text(strip=True)
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：重组 yy (年份) 和 mmdd (月-日) -> YYYY-MM-DD
                    date_text = ""
                    yy = a.select_one('.yy')
                    mmdd = a.select_one('.mmdd')
                    if yy and mmdd:
                        year = yy.get_text(strip=True)
                        month_day = mmdd.get_text(strip=True).replace('/', '-')
                        if year and month_day:
                            date_text = f"{year}-{month_day}"

                    # 兜底
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 商丘师院 .fylb 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：沈阳大学 yjs.syu.edu.cn line_u9 + h2 复合结构 --------------------
        syu_line9_items = soup.select('li[id^="line_u9_"]')
        if syu_line9_items:
            # 校验该卡片列表首项是否具有特征日期类 span i 与标题类 h2
            first_item = syu_line9_items[0]
            if first_item.select_one('span i') and first_item.select_one('h2'):
                current = []

                # 安全基准地址
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for li in syu_line9_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 h2 中的标题文本
                    h2 = a.select_one('h2')
                    title = h2.get_text(strip=True) if h2 else a.get_text(strip=True)

                    # 清除可能带有的尾部省略号或杂质
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：span 内部的 i 标签 (日) 与外部的年月 (年-月)
                    date_text = ""
                    span_date = li.select_one('span')
                    if span_date:
                        i_tag = span_date.select_one('i')
                        full_span_text = span_date.get_text(strip=True)  # 获取连读文本，如 "192026-03"
                        if i_tag:
                            day = i_tag.get_text(strip=True)
                            # 从连读文本中剥离天数（仅替换一次，防范年份中也有相同数字时被误删），提纯年月信息
                            year_month = full_span_text.replace(day, "", 1).replace('/', '-').strip()
                            if day and year_month:
                                # 补零合并为标准格式 YYYY-MM-DD
                                date_text = f"{year_month}-{day.zfill(2)}"

                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    # 统一核算时间差与日期标准清洗
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 沈阳大学 line_u9 策略定位命中 → {len(current)} 条")
                    return current

            # -------------------- 规则：河海大学等 .ahref / data-url 无 A 标签动态卡片结构 --------------------
        dynamic_items = soup.select('#wp_news_w3 .item, .item:has(.ahref), .item:has([data-url])')
        if not dynamic_items:
            # 兜底：精准筛选含有日期、标题及 data-url 属性的所有 div.item 节点
            dynamic_items = [div for div in soup.find_all('div', class_='item') if
                             div.select_one('.month') and div.select_one('.title')]

        # -------------------- 规则：河海大学等 .ahref / data-url 无 A 标签动态卡片结构 --------------------
        dynamic_items = soup.select('#wp_news_w3 .item, .item:has(.ahref), .item:has([data-url])')
        if not dynamic_items:
            # 兜底：精准筛选含有日期、标题及 data-url 属性的所有 div.item 节点
            dynamic_items = [div for div in soup.find_all('div', class_='item') if
                             div.select_one('.month') and div.select_one('.title')]

        if dynamic_items:
            # 校验首个元素特征，必须含有 .month, .day 且有 .title 标题
            first_item = dynamic_items[0]
            if first_item.select_one('.month') and first_item.select_one('.day') and first_item.select_one(
                    '.title'):
                current = []

                # 安全基准地址
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for div in dynamic_items:
                    # 提取标题
                    title_div = div.select_one('.title')
                    if not title_div:
                        continue
                    title = title_div.get_text(strip=True)
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回', '查看详情']:
                        continue

                    # 提取链接：由于没有 <a> 标签，必须从带有 data-url 属性的节点中提取
                    url = ""
                    url_node = div.select_one('[data-url]')
                    if url_node:
                        url_raw = url_node.get('data-url', '').strip()
                        if url_raw:
                            url = urljoin(local_base_url, url_raw)

                    if not url:
                        continue

                    # 日期提取：合并 month (年-月，如 2026-03) 与 day (日，如 24) -> YYYY-MM-DD
                    date_text = ""
                    month_div = div.select_one('.month')
                    day_div = div.select_one('.day')
                    if month_div and day_div:
                        month_val = month_div.get_text(strip=True)
                        day_val = day_div.get_text(strip=True)
                        if month_val and day_val:
                            date_text = f"{month_val}-{day_val.zfill(2)}"

                    # 兜底
                    if not date_text:
                        date_text = self._extract_date_from_element(div)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 动态 data-url .ahref 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：中原工学院 .l2-q / d1+d2 拆分日期结构 --------------------
        l2_items = soup.select('ul.l2-q li, .l2-q li')
        if l2_items:
            # 校验首个元素特征，必须含有 .d1, .d2 且有 h4 标题
            first_item = l2_items[0]
            if first_item.select_one('.d1') and first_item.select_one('.d2') and first_item.select_one('h4'):
                current = []

                # 安全基准地址
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for li in l2_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题
                    h4 = a.select_one('h4')
                    title = h4.get_text(strip=True) if h4 else a.get('title', '').strip()
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：重组 d1 (年份) 和 d2 (月-日) -> YYYY-MM-DD
                    date_text = ""
                    d1 = a.select_one('.d1')
                    d2 = a.select_one('.d2')
                    if d1 and d2:
                        year = d1.get_text(strip=True)
                        month_day = d2.get_text(strip=True).replace('/', '-')
                        if year and month_day:
                            date_text = f"{year}-{month_day}"

                    # 兜底
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 中原工学院 .l2-q 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：河南中医药大学 .right_content_item 列表卡片结构 --------------------
        rc_items = soup.select('.right_content_item, a.right_content_item')
        if rc_items:
            # 校验首个元素特征，必须含有 item_content_con 标题类或 item_content 容器
            first_item = rc_items[0]
            if first_item.select_one('.item_content_con') or first_item.select_one('.item_content'):
                current = []

                # 安全基准地址
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for item in rc_items:
                    a = item if item.name == 'a' else item.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题
                    title_p = item.select_one('.item_content_con') or item.select_one('p')
                    title = title_p.get_text(strip=True) if title_p else a.get('title', '').strip()
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：优先读取 class="item_day" 标签上的 date 属性，避免解析拼接误差
                    date_text = ""
                    day_div = item.select_one('.item_day')
                    month_div = item.select_one('.item_month')

                    if day_div and day_div.get('date'):
                        date_text = day_div.get('date').strip()
                    elif month_div and month_div.get('date'):
                        date_text = month_div.get('date').strip()

                    # 属性读取失败时，退回通用提取器
                    if not date_text:
                        date_text = self._extract_date_from_element(item)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 河南中医药大学 .right_content_item 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：通用无类名 plain_list 极简列表结构 --------------------
        # 特别适合 ul 和 li 标签完全没有任何 class 类名修饰的极其质朴、干净的列表排版
        plain_items = soup.select('ul li, ol li')
        if plain_items:
            # 校验该朴素列表首行是否具有特征 a 标签与带时间特征的 span 节点
            first_item = None
            for item in plain_items:
                if item.find('a', href=True) and (
                        item.select_one('span.date') or item.select_one('.date') or item.select_one('span')):
                    first_item = item
                    break

            if first_item:
                current = []

                # ==================== 【安全防线：在规则内部直接定义基准地址】 ====================
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url
                # =========================================================================

                for li in plain_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    # 清除可能存在的末尾省略号
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    # 使用本地安全解析出的基准地址进行拼接，永不报错
                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：优先查找带 date 样式的 span，其次查找第一个包含 4 位年份数字的 span
                    date_text = ""
                    span_tag = li.select_one('span.date') or li.select_one('.date')
                    if not span_tag:
                        for s in li.find_all('span'):
                            s_text = s.get_text(strip=True)
                            if re.search(r'\d{4}', s_text):
                                span_tag = s
                                break

                    date_text = span_tag.get_text(strip=True) if span_tag else self._extract_date_from_element(
                        li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    # 统一核算时间差与日期标准清洗
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 通用无类名 plain_list 策略命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：武汉轻工大 .alld + a + p + div 结构 --------------------
        alld_items = soup.select('.alld a, div.alld a')
        if not alld_items:
            # 兜底：反向匹配任何直接包裹 p 标签和 div 标签的 a 链接
            alld_items = [a for a in soup.select('a') if a.select_one('p') and a.select_one('div')]

        if alld_items:
            # 校验该卡片列表首行是否具有特征标题 p 与日期 div
            first_item = alld_items[0]
            if first_item.select_one('p') and first_item.select_one('div'):
                current = []

                # 安全防线：在规则内部直接定义基准地址
                base_tag = soup.find('base', href=True)
                local_base_url = base_tag['href'].strip() if base_tag else base_url

                for a in alld_items:
                    # 优先获取 p 标签完整的 title 属性，防字符溢出截断
                    p_tag = a.select_one('p')
                    title = p_tag.get('title', '').strip() if p_tag else ''
                    if not title:
                        title = p_tag.get_text(strip=True) if p_tag else a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    # 利用安全对齐后的基准路径拼接
                    url = urljoin(local_base_url, a.get('href', '').strip())

                    # 日期提取：直接读取第一个子 div
                    date_div = a.select_one('div')
                    date_text = date_div.get_text(strip=True) if date_div else self._extract_date_from_element(a)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    # 统一核算时间差
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 武汉轻工大 .alld 策略命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：徐医大医学技术院 .nyr + a[id^="line_u"] 结构 --------------------
        nyr_items = soup.select('.nyr dl a[id^="line_u"], .nyr a[id^="line_u"]')
        if nyr_items:
            # 校验该卡片列表首行是否具有特征 strong 标签
            first_item = nyr_items[0]
            if first_item.select_one('dd p strong') or first_item.select_one('strong'):
                current = []
                for a in nyr_items:
                    # 日期提取：强力锁定 dd p strong
                    strong = a.select_one('dd p strong') or a.select_one('strong')
                    date_text = strong.get_text(strip=True) if strong else ""

                    # 标题精细化降噪：从链接总文本中，完美剔除最前方紧密粘连的日期
                    title = a.get_text(strip=True)
                    if date_text:
                        title = title.replace(date_text, "").strip()

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    # 统一核算时间差与日期标准清洗
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 徐医大医学技术院 .nyr 策略命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：通用 .ny-list / .nav-list 极简自适应列表结构 --------------------
        # 泛化选择器，完美一键兼容 '.ny-list'（成体）与 '.nav-list'（吉大二院）及所有含同级 span 按钮的极简列表
        ny_list_items = soup.select('.ny-list li, .nav-list li, div.ny-list li, div.nav-list li')
        if ny_list_items:
            # 校验该卡片列表首项是否具有特征 a 标签与同级 span.date/span
            first_item = ny_list_items[0]
            if first_item.find('a', href=True) and (
                    first_item.select_one('span.date') or first_item.select_one('span')):
                current = []
                for li in ny_list_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    # 强力清除部分被前端裁切导致尾部带有的省略号：如 "..." 或 "…"
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：span.date 或 span 标签
                    span = li.select_one('span.date') or li.select_one('span')
                    date_text = span.get_text(strip=True) if span else self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    # 统一核算时间差与日期标准清洗
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 通用 .ny-list/.nav-list 策略命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：山师大通用 .d1-list + p.tit 结构 --------------------
        d1_items = soup.select('ul.d1-list li, .d1-list li')
        if d1_items:
            # 校验该卡片列表首项是否具有特征标题类 p.tit 与日期类 p.date
            first_item = d1_items[0]
            if first_item.select_one('p.tit') and first_item.select_one('p.date'):
                current = []
                for li in d1_items:
                    # 兼容：有些是在 li 外面包裹 a，有些是在内部
                    a = li.find_parent('a') or li.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题
                    title_p = li.select_one('p.tit') or li.select_one('.tit')
                    title = title_p.get_text(strip=True) if title_p else a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：p.date 内的 span 标签
                    date_span = li.select_one('p.date span') or li.select_one('.date span') or li.select_one(
                        '.date')
                    date_text = date_span.get_text(
                        strip=True) if date_span else self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    # 统一核算时间差
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 山师大通用 .d1-list 策略命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：通用 .xinwen_list 极简文字列表结构 --------------------
        xinwen_items = soup.select('.xinwen_list li, div.xinwen_list li')
        if xinwen_items:
            # 校验该卡片列表首行是否具有特征 a 标签与右侧 span
            first_item = xinwen_items[0]
            if first_item.find('a', href=True) and first_item.select_one('span'):
                current = []
                for li in xinwen_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签完整的 title 属性
                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    # 强力净化：合并多余空行与空格
                    title = re.sub(r'[\s\xa0\u3000\n\r]+', ' ', title).strip()

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：span 标签，使用 .strip('[]') 去除可能存在的中括号
                    span_tag = li.select_one('span')
                    date_text = span_tag.get_text(strip=True).strip(
                        '[]') if span_tag else self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    # 统一核算时间差
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 通用 .xinwen_list 策略命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：山师大商学院 .he2_cntzriboli 复合结构 --------------------
        sdnu_items = soup.select('ul.he2_cntzriboul li, .he2_cntzriboli')
        if sdnu_items:
            # 校验该卡片列表首行是否具有特征日期类 .ca_news_date 与标题类 .he2_title5
            first_item = sdnu_items[0]
            if first_item.select_one('.ca_news_date') and first_item.select_one('.he2_title5'):
                current = []
                for li in sdnu_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题（位于 .he2_title5 中）
                    h4 = a.select_one('.he2_title5') or a.select_one('h4')
                    title = h4.get_text(strip=True) if h4 else a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：.ca_news_date 内部的 .ca_news_ym (如 2026-05) 与 .ca_news_dd (如 26)
                    date_text = ""
                    date_div = a.select_one('.ca_news_date')
                    if date_div:
                        dd = date_div.select_one('.ca_news_dd p') or date_div.select_one('.ca_news_dd')
                        ym = date_div.select_one('.ca_news_ym p') or date_div.select_one('.ca_news_ym')
                        if dd and ym:
                            day = dd.get_text(strip=True)
                            year_month = ym.get_text(strip=True).replace('/', '-')
                            if day and year_month:
                                # 补零合并为标准格式 YYYY-MM-DD
                                date_text = f"{year_month}-{day.zfill(2)}"

                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    # 重新计算并格式化时间
                    self._recalc_current_dates(current)

                    # 【核心去噪升级】：利用正则表达式，自动擦除标题头部连带的“活动举办日期”前缀（如 2026.05.28）
                    for item in current:
                        title = item['title']
                        # 擦除开头的 YYYY.MM.DD、YYYY-MM-DD 形式的干扰时间前缀
                        title = re.sub(r'^\d{4}[-./]\d{1,2}[-./]\d{1,2}\s*', '', title).strip()
                        # 兼容 YYYY.M.DD 形式的干扰时间前缀
                        title = re.sub(r'^\d{4}[-./]\d{1,2}[-./]\d{1,2}\s*', '', title).strip()
                        item['title'] = title

                    self.log.emit(f"[手工规则] 山师大商学院 .he2_cntzriboli 策略命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：天职师大艺术院 .tzgg + a.wl 复合结构 --------------------
        tzgg_items = soup.select('.tzgg li, div.tzgg li')
        if tzgg_items:
            # 校验该卡片列表首项是否具有特征 a.wl 链接与特征日期结构 .date span
            first_item = tzgg_items[0]
            if first_item.select_one('a.wl') and first_item.select_one('.date span'):
                current = []
                for li in tzgg_items:
                    a = li.select_one('a.wl') or li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                    title = a.get('title', '').strip()
                    if not title:
                        h3 = a.select_one('.con h3') or a.select_one('h3')
                        title = h3.get_text(strip=True) if h3 else a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期重组：提取 .date 内的 span (如 2026) 并剥离后拼接外围月日 (如 04-01)
                    date_text = ""
                    date_div = a.select_one('.date')
                    if date_div:
                        span = date_div.select_one('span')
                        if span:
                            year = span.get_text(strip=True)
                            full_text = date_div.get_text(strip=True)
                            # 从连读文本中刨除年份，提纯月日
                            date_part = full_text.replace(year, '').strip()
                            if year.isdigit() and re.match(r'^\d{1,2}[-/.]\d{1,2}$', date_part):
                                # 拼合为标准 YYYY-MM-DD
                                date_text = f"{year}-{date_part.replace('.', '-')}"

                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    # 重新计算并格式化时间
                    for item in current:
                        formatted_date, days_old, success = self._parse_date(item['date'])
                        item['date'] = formatted_date if success else item['date']
                        item['days_old'] = days_old

                    self.log.emit(f"[成功] 天职师大艺术院 .tzgg 策略命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：山大商学院 .lb-ul + .zt-ul-title 结构 --------------------
        sdu_items = soup.select('ul.lb-ul li, .lb-ul li')
        if sdu_items:
            # 校验该卡片列表首行是否具有特征日期类 .zt-ul-date 与标题类 .zt-ul-title
            first_item = sdu_items[0]
            if first_item.select_one('.zt-ul-date') and first_item.select_one('.zt-ul-title'):
                current = []
                for li in sdu_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                    title = a.get('title', '').strip()
                    if not title:
                        title_div = a.select_one('.zt-ul-title')
                        title = title_div.get_text(strip=True) if title_div else a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：.zt-ul-date 内部的 span 和 font 标签进行合流重组
                    date_text = ""
                    date_div = a.select_one('.zt-ul-date')
                    if date_div:
                        span_dm = date_div.select_one('span')
                        font_yr = date_div.select_one('font')
                        if span_dm and font_yr:
                            dm = span_dm.get_text(strip=True)
                            year = font_yr.get_text(strip=True)
                            if dm and year:
                                # 标准化拼合为 YYYY-MM-DD
                                date_text = f"{year}-{dm}"

                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    # 统一核算时间差
                    self._recalc_current_dates(current)
                    self.log.emit(f"[手工规则] 山大商学院 .lb-ul 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：天职师大 .text-list + .spanys 复合嵌套结构 ====================
        tute_items = soup.select('.text-list li, div.text-list li')
        if tute_items:
            # 校验该卡片列表首行是否具有特征标题类与特征时间 spanys 节点
            first_item = tute_items[0]
            if first_item.select_one('.timg-linfo') and first_item.select_one('.spanys'):
                current = []
                for li in tute_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 1. 提取并精准重组拆分日期：<span class="spanys"><b>15</b>/ 2026-05</span>
                    date_text = ""
                    spanys = a.select_one('.spanys')
                    if spanys:
                        b_day = spanys.select_one('b')
                        full_span_text = spanys.get_text(strip=True)  # 如 "15/2026-05"
                        if b_day:
                            day = b_day.get_text(strip=True)
                            # 从连读文本中刨除天数与斜杠，提纯年月信息
                            year_month = full_span_text.replace(day, "").replace('/', '').strip()
                            if day and year_month:
                                # 补零合并为标准格式 YYYY-MM-DD
                                date_text = f"{year_month}-{day.zfill(2)}"

                    # 2. 提取标题并降噪：复制 h3 并对其内部的 .spanys 日期标签进行物理剥离，防标题污染
                    h3 = a.select_one('.timg-linfo h3') or a.select_one('h3')
                    if h3:
                        h3_copy = BeautifulSoup(str(h3), 'html.parser').select_one('h3')
                        spanys_copy = h3_copy.select_one('.spanys')
                        if spanys_copy:
                            spanys_copy.decompose()  # 剔除复制出来的日期节点
                        title = h3_copy.get_text(strip=True)
                    else:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 兜底时间
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    # 统一核算时间差
                    self._recalc_current_dates(current)
                    self.log.emit(f"[成功] 天职师大 .text-list 策略定位命中 → {len(current)} 条")
                    return current

        # -------------------- 规则：天师大法学院 .l_new_box 列表结构 --------------------
        l_new_items = soup.select('.l_new_box, div.l_new_box')
        if l_new_items:
            # 校验该卡片列表首项是否具有特征日期类 .l_new_leyaar 与标题类 .l_new_tit
            first_item = l_new_items[0]
            if first_item.select_one('.l_new_leyaar') and first_item.select_one('.l_new_tit'):
                current = []
                for div in l_new_items:
                    a = div.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题（优先从 .l_new_tit 提取）
                    tit_div = a.select_one('.l_new_tit') or a.select_one('.l_new_text')
                    title = tit_div.get_text(strip=True) if tit_div else a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期重组：.l_new_year (如 2022-02) + .l_new_riqi (如 26)
                    date_text = ""
                    date_div = a.select_one('.l_new_leyaar')
                    if date_div:
                        day_div = date_div.select_one('.l_new_riqi')
                        year_div = date_div.select_one('.l_new_year')
                        if day_div and year_div:
                            day = day_div.get_text(strip=True)
                            year_month = year_div.get_text(strip=True).replace('/', '-')
                            if day and year_month:
                                # 补零合并为标准格式 YYYY-MM-DD
                                date_text = f"{year_month}-{day.zfill(2)}"

                    if not date_text:
                        date_text = self._extract_date_from_element(div)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    # 重新计算 days_old 和格式化时间，防止未来时间漏洞
                    for item in current:
                        formatted_date, days_old, success = self._parse_date(item['date'])
                        item['date'] = formatted_date if success else item['date']
                        item['days_old'] = days_old

                    self.log.emit(f"[成功] 天师大法学院 .l_new_box 策略定位命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：吉大二院 .nav-list + a + span 极简卡片结构 ====================
        nav_list_items = soup.select('.nav-list')
        if nav_list_items:
            # 校验该卡片列表首行是否具有特征 a 标签与右侧同级 span
            first_item = nav_list_items[0]
            if first_item.find('a', href=True) and first_item.select_one('span'):
                current = []
                for div in nav_list_items:
                    a = div.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签完整的 title 属性
                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    # 强力清除部分被前端裁切导致尾部带有的省略号：如 "..." 或 "…"
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期直接提取自同级 span
                    span = div.select_one('span')
                    date_text = span.get_text(strip=True) if span else self._extract_date_from_element(div)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self._recalc_current_dates(current)
                    self.log.emit(f"[成功] 吉大二院 .nav-list 策略命中 → {len(current)} 条")
                    return current

        # ==================== 新增适配：合工大 .news-list + .media 复合拆分日期结构 ====================
        media_links = soup.select('.news-list a:has(.media), .news-list a')
        if media_links:
            # 校验该卡片列表首行是否具有特征日期结构
            first_item = media_links[0]
            if first_item.select_one('.media') and first_item.select_one('.date b') and first_item.select_one(
                    'h5'):
                current = []
                for a in media_links:
                    if not a.select_one('.media'):
                        continue

                    # 提取标题
                    h5 = a.select_one('h5')
                    title = h5.get_text(strip=True) if h5 else a.get('title', '').strip()
                    if not title:
                        continue

                    # 净化换行符与尾部省略号噪声
                    title = re.sub(r'[\s\xa0\u3000\n\r]+', ' ', title).strip()
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：重组 .date 内部的 b 标签 (如 22) 与剩余的年份文本 (如 2024-06)
                    date_text = ""
                    date_div = a.select_one('.date')
                    if date_div:
                        b_tag = date_div.select_one('b')
                        if b_tag:
                            day = b_tag.get_text(strip=True)
                            full_text = date_div.get_text(" ", strip=True)  # 获取完整行文本如 "22 2024-06"
                            # 在连读文本中刨除天数并转化为连字符
                            year_month = full_text.replace(day, "").replace('/', '-').strip()
                            if day and year_month:
                                # 补零并合并为标准格式 YYYY-MM-DD
                                date_text = f"{year_month}-{day.zfill(2)}"

                    # 兜底提取
                    if not date_text:
                        date_text = self._extract_date_from_element(a)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] .news-list + .media 策略定位命中 → {len(current)} 条")
                    return current

        # ==================== 新增适配：厦门大学等 .inner-news-list 复合拆分日期结构 ====================
        inner_news_items = soup.select('ul.inner-news-list li, .inner-news-list li')
        if inner_news_items:
            # 校验该卡片列表首行是否具有特征日期结构 .date-box 且包含详情标题与链接
            first_item = inner_news_items[0]
            if first_item.select_one('.date-box') and (
                    first_item.select_one('.title') or first_item.select_one('h2')) and first_item.find('a',
                                                                                                        href=True):
                current = []
                for li in inner_news_items:
                    a = li.select_one('a.link') or li.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题
                    title_elem = li.select_one('h2.title') or li.select_one('.title') or li.select_one('h2')
                    title = title_elem.get_text(strip=True) if title_elem else a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    # 净化换行符与尾部省略号噪声
                    title = re.sub(r'[\s\xa0\u3000\n\r]+', ' ', title).strip()
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：重组 .date-box 内部的 .sub (年份) 和 .main (月-日)
                    date_text = ""
                    date_box = li.select_one('.date-box')
                    if date_box:
                        sub_span = date_box.select_one('.sub')
                        main_span = date_box.select_one('.main')
                        if sub_span and main_span:
                            year = sub_span.get_text(strip=True)
                            month_day = main_span.get_text(strip=True).replace('/', '-')
                            if year and month_day:
                                # 合并为标准格式 YYYY-MM-DD
                                date_text = f"{year}-{month_day}"

                    # 兜底提取
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] .inner-news-list 复合策略定位命中 → {len(current)} 条")
                    return current

        # ==================== 新增适配：厦大海洋院 .ul-dzry / .notice-card 复合拆分日期结构 ====================
        notice_cards = soup.select('.notice-card, li.li-dzry')
        if notice_cards:
            # 校验该卡片列表首行是否具有特征日期结构
            first_item = notice_cards[0]
            if first_item.select_one('.notice-card__date') and first_item.select_one(
                    '.notice-card__title') and first_item.select_one('.notice-card__link'):
                current = []
                for card in notice_cards:
                    a = card.select_one('.notice-card__link') or card.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题
                    title_elem = card.select_one('.notice-card__title')
                    title = title_elem.get_text(strip=True) if title_elem else a.get('title',
                                                                                     '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    # 净化换行符与尾部省略号噪声
                    title = re.sub(r'[\s\xa0\u3000\n\r]+', ' ', title).strip()
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：重组 .notice-card__day (如 28) 与 .notice-card__month (如 2026/03)
                    date_text = ""
                    day_elem = card.select_one('.notice-card__day')
                    month_elem = card.select_one('.notice-card__month')
                    if day_elem and month_elem:
                        day = day_elem.get_text(strip=True)
                        year_month = month_elem.get_text(strip=True).replace('/', '-')
                        if day and year_month:
                            # 补零并合并为标准格式 YYYY-MM-DD
                            date_text = f"{year_month}-{day.zfill(2)}"

                    # 兜底提取
                    if not date_text:
                        date_text = self._extract_date_from_element(card)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] .ul-dzry / .notice-card 策略定位命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：华电电气院 .subNoticeList 复合拆分日期结构 ====================
        ncepu_items = soup.select('ul.subNoticeList li, .subNoticeList li')
        if ncepu_items:
            # 校验该卡片列表首行是否具有特征日期结构 .dateLeft 与特征标题节点 .Notice_info
            first_item = ncepu_items[0]
            if first_item.select_one('.dateLeft') and first_item.select_one('.Notice_info'):
                current = []
                for li in ncepu_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题
                    h3 = a.select_one('.Notice_info h3') or a.select_one('h3')
                    title = h3.get_text(strip=True) if h3 else a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    # 强力净化：合并多行文本间的物理换行与不换行空格，并切除尾部带有的省略号
                    title = re.sub(r'[\s\xa0\u3000\n\r]+', ' ', title).strip()
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：重组 .dateLeft 内的 i (如 2026.04) 与 strong (如 13)
                    date_text = ""
                    date_span = a.select_one('.dateLeft')
                    if date_span:
                        strong = date_span.select_one('strong')
                        i_tag = date_span.select_one('i')
                        if strong and i_tag:
                            day = strong.get_text(strip=True)
                            year_month = i_tag.get_text(strip=True).replace('.', '-')
                            if day and year_month:
                                # 补零合并为标准格式 YYYY-MM-DD
                                date_text = f"{year_month}-{day.zfill(2)}"

                    # 兜底提取
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 华电电气院 .subNoticeList 策略定位命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：华北理工冶金能院 .sy_list_n 复合结构 ====================
        sy_items = soup.select('.sy_list_n, div.sy_list_n')
        if sy_items:
            # 校验该卡片列表首行是否具有特征类 .sy_list_n1 以及包含有效 a
            first_item = sy_items[0]
            if first_item.select_one('.sy_list_n1 a') and first_item.find('a', href=True):
                current = []
                for div in sy_items:
                    a = div.select_one('.sy_list_n1 a') or div.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取双通道：优先从 href 中提取 /YYYY/MM/DD/，其次提取 float:right 节点剥离括号
                    date_text = ""
                    m_url = re.search(r'/(\d{4})/(\d{2})/(\d{2})/', a.get('href', ''))
                    if m_url:
                        date_text = f"{m_url.group(1)}-{m_url.group(2)}-{m_url.group(3)}"

                    if not date_text:
                        right_div = div.find('div', style=lambda s: s and 'float:right' in s.replace(' ', ''))
                        if right_div:
                            date_text = right_div.get_text(strip=True).strip('[]')
                        else:
                            date_text = self._extract_date_from_element(div)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 华北理工冶金能院 .sy_list_n 策略定位命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：华北理工公卫院 .right-bd.detail.news 结构 ====================
        hebut_items = soup.select('.right-bd.detail.news li, .detail.news li')
        if not hebut_items:
            # 兜底：寻找包含 span.date 且有 a 标签的 li 列表
            hebut_items = [li for li in soup.select('li') if li.select_one('span.date') and li.find('a', href=True)]

        if hebut_items:
            # 校验首个元素是否符合特征布局（包含带 .date 样式的 span 容器和链接）
            first_item = hebut_items[0]
            if first_item.select_one('span.date') and first_item.find('a', href=True):
                current = []
                for li in hebut_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    href_raw = a.get('href', '').strip()
                    url = urljoin(base_url, href_raw)

                    # 高优策略：由于链接中内嵌了完整日期，优先从中用正则提取 YYYY/MM/DD
                    date_text = ""
                    m_url = re.search(r'/(\d{4})/(\d{2})/(\d{2})/', href_raw)
                    if m_url:
                        date_text = f"{m_url.group(1)}-{m_url.group(2)}-{m_url.group(3)}"

                    # 兜底策略：若为外链（如微信公众号），提取带括号的 [MM-DD] 文本，由底层智能倒推器拼合年份
                    if not date_text:
                        date_span = li.select_one('span.date') or li.select_one('.date')
                        date_text = date_span.get_text(strip=True).strip(
                            '[]') if date_span else self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 华北理工公卫院 .right-bd 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：4水电大学 .news-item 复合卡片结构（通用双模版） ====================
        ncwu_items = soup.select('.news-list .news-item, .news-item')
        if ncwu_items:
            # 校验该卡片列表首行是否具有特征结构（包含 a 详情链接与时间/属性特征）
            first_item = ncwu_items[0]
            if first_item.select_one('.description h2 a') or first_item.find('a', href=True):
                current = []
                for item in ncwu_items:
                    desc = item.select_one('.description')
                    a = desc.select_one('h2 a') if desc else item.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回', '查看更多']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取（双重兼容模式）：
                    # 模式 A：.date 内部拆分为 .month (年-月) + .day (天)
                    date_text = ""
                    date_div = item.select_one('.date')
                    if date_div:
                        day_div = date_div.select_one('.day')
                        month_div = date_div.select_one('.month')
                        if day_div and month_div:
                            day = day_div.get_text(strip=True)
                            year_month = month_div.get_text(strip=True).replace('/', '-')
                            if day and year_month:
                                date_text = f"{year_month}-{day.zfill(2)}"

                    # 模式 B：.date 仅为索引，真正日期在 .attr 属性行中（如：时间：2026-05-13）
                    if not date_text:
                        attr_div = item.select_one('.attr')
                        if attr_div:
                            attr_text = attr_div.get_text(strip=True)
                            m_date = re.search(r'(\d{4}[-/.]\d{1,2}[-/.]\d{1,2})', attr_text)
                            if m_date:
                                date_text = m_date.group(1).replace('/', '-')

                    # 模式 C：前两者均失败时，进行通用兜底提取
                    if not date_text:
                        date_text = self._extract_date_from_element(item)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 华北水水电 .news-item 策略定位命中（双模日期） → {len(current)} 条")
                    return current

        # ==================== 1. 新增：华北水利水电大学公管院 li[onclick] 列表结构 ====================
        # 特别针对通过 onclick="location='...'" 进行 JS 动态跳转、不含 a 标签的列表
        ncwu_pa_items = soup.select('ul.list-text li[onclick], li[onclick]:has(.title)')
        if not ncwu_pa_items:
            # 兜底：寻找带有 .title 且绑定了 onclick 的行级元素
            ncwu_pa_items = [li for li in soup.select('li') if
                             li.get('onclick') and (li.select_one('.title') or li.select_one('h5'))]

        if ncwu_pa_items:
            # 确认其首项是否具备特征子样式
            first_item = ncwu_pa_items[0]
            if first_item.get('onclick') and (first_item.select_one('.title') or first_item.select_one('h5')):
                current = []
                for li in ncwu_pa_items:
                    # 从 onclick 属性中利用正则剥离出跳转链接
                    onclick = li.get('onclick', '')
                    match = re.search(r"location\s*=\s*['\"](.*?)['\"]", onclick)
                    if not match:
                        # 兜底寻找：防止未来改版加入了 standard a 标签
                        a = li.find('a', href=True)
                        if a:
                            url = urljoin(base_url, a.get('href', '').strip())
                        else:
                            continue
                    else:
                        url = urljoin(base_url, match.group(1).strip())

                    # 提取标题
                    h5 = li.select_one('h5.title') or li.select_one('h5') or li.select_one('.title')
                    title = h5.get_text(strip=True) if h5 else ""
                    if not title:
                        title = li.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, url)

                    # 提取日期：span.time
                    span_time = li.select_one('span.time') or li.select_one('.time') or li.select_one('span')
                    date_text = span_time.get_text(strip=True) if span_time else self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 华北水水电公管院 li[onclick] 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：华北水利水电大学 .news-item 复合卡片结构 ====================
        ncwu_items = soup.select('.news-list .news-item, .news-item')
        if ncwu_items:
            # 校验该卡片列表首行是否具有特征日期结构 .date 且包含详情 h2 a 链接
            first_item = ncwu_items[0]
            if first_item.select_one('.date .day') and first_item.select_one('.description h2 a'):
                current = []
                for item in ncwu_items:
                    desc = item.select_one('.description')
                    a = desc.select_one('h2 a') if desc else item.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回', '查看更多']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：重组 .date 内部的 .month (如 2026-04) 和 .day (如 30)
                    date_text = ""
                    date_div = item.select_one('.date')
                    if date_div:
                        day_div = date_div.select_one('.day')
                        month_div = date_div.select_one('.month')
                        if day_div and month_div:
                            day = day_div.get_text(strip=True)
                            year_month = month_div.get_text(strip=True).replace('/', '-')
                            if day and year_month:
                                # 补零合并为标准格式 YYYY-MM-DD
                                date_text = f"{year_month}-{day.zfill(2)}"

                    # 兜底提取
                    if not date_text:
                        date_text = self._extract_date_from_element(item)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 华北水水电 .news-item 策略定位命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：北师大人居学院 li[onclick] 媒体列表结构 ====================
        # 特别针对通过 onclick="location='...'" 进行 JS 动态跳转、不含 a 标签的媒体列表
        media_items = soup.select('ul.media-list li.media, li.media')
        if not media_items:
            # 兜底：寻找带有 .media-heading 且绑定了 onclick 的行级元素
            media_items = [li for li in soup.select('li') if li.select_one('.media-heading') and li.get('onclick')]

        if media_items:
            # 确认其首项是否具备特征子样式
            first_item = media_items[0]
            if first_item.select_one('.media-heading') and first_item.get('onclick'):
                current = []
                for li in media_items:
                    # 从 onclick 属性中利用正则剥离出跳转链接
                    onclick = li.get('onclick', '')
                    match = re.search(r"location\s*=\s*['\"](.*?)['\"]", onclick)
                    if not match:
                        # 兜底寻找：防止未来改版加入了 standard a 标签
                        a = li.find('a', href=True)
                        if a:
                            url = urljoin(base_url, a.get('href', '').strip())
                        else:
                            continue
                    else:
                        url = urljoin(base_url, match.group(1).strip())

                    # 提取标题
                    h4 = li.select_one('.media-heading')
                    title = h4.get_text(strip=True) if h4 else ""
                    if not title:
                        title = li.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    # 提取日期：.media-body 下的 p
                    p_tag = li.select_one('.media-body p') or li.select_one('p')
                    date_text = p_tag.get_text(strip=True) if p_tag else self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 北师大人居学院 li[onclick] 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：华侨大学计算机院 .newlist1 + a.wl 自适应卡片结构 ====================
        newlist1_items = soup.select('.newlist1 li, div.newlist1 li')
        if newlist1_items:
            # 校验该卡片列表首行是否具有特征 a.wl 链接与特征类名 .txt1
            first_item = newlist1_items[0]
            if first_item.select_one('a.wl') and first_item.select_one('.txt1'):
                current = []
                for li in newlist1_items:
                    a = li.select_one('a.wl') or li.find('a', href=True)
                    if not a:
                        continue

                    # 强力技巧：优先获取 a 标签上未被前端省略号截断的 title 完整属性值
                    title = a.get('title', '').strip()
                    if not title:
                        h3 = a.select_one('.txt1') or a.select_one('h3')
                        title = h3.get_text(strip=True) if h3 else a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：直接读取 a 标签内部底部的 span 节点
                    span_date = a.select_one('span')
                    date_text = span_date.get_text(strip=True) if span_date else self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 华侨大学计算机院 .newlist1 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：华侨大学 .txtList 复合拆分日期结构 ====================
        hqu_txt_items = soup.select('ul.txtList li, .txtList li')
        if hqu_txt_items:
            # 校验该卡片列表首行是否具有特征日期结构 .time span 与标题类 h4.eclips 标签
            first_item = hqu_txt_items[0]
            if first_item.select_one('.time span') and (
                    first_item.select_one('h4.eclips') or first_item.select_one('h4')):
                current = []
                for li in hqu_txt_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                    title = a.get('title', '').strip()
                    if not title:
                        h4 = a.select_one('h4.eclips') or a.select_one('h4')
                        title = h4.get_text(strip=True) if h4 else a.get_text(strip=True)

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：重组 .time 内的 span (如 13) 与其余文本 (如 2026.03)
                    date_text = ""
                    time_div = a.select_one('.time')
                    if time_div:
                        day_span = time_div.select_one('span')
                        full_text = time_div.get_text(strip=True)  # 获取诸如 "132026.03" 的连读文本
                        if day_span:
                            day = day_span.get_text(strip=True)
                            # 从连读文本中刨除天数，并将点号转换为连字符
                            year_month = full_text.replace(day, "").replace('.', '-').strip()
                            if day and year_month:
                                # 补零合并为标准格式 YYYY-MM-DD
                                date_text = f"{year_month}-{day.zfill(2)}"

                    # 兜底提取
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 华侨大学 .txtList 策略定位命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：厦大医学院 .n_list + li.alin_r 复合结构 ====================
        n_list_items = soup.select('.n_list li, div.n_list li')
        if n_list_items:
            # 校验该卡片列表首行是否具有特征日期类 time 与标题类 .aline_r
            first_item = n_list_items[0]
            if first_item.select_one('.aline_r') and first_item.select_one('time'):
                current = []
                for li in n_list_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                    title = a.get('title', '').strip()
                    if not title:
                        title_span = a.select_one('.aline_r') or a.select_one('span')
                        title = title_span.get_text(strip=True) if title_span else a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：直接读取 time
                    time_tag = li.select_one('time')
                    date_text = time_tag.get_text(strip=True) if time_tag else self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 厦大医学院 .n_list 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：华中科大软件院 .text-listx + a.lbyy 复合列表结构 ====================
        se_items = soup.select('.text-listx li, .text-list li')
        if se_items:
            # 校验该卡片列表首行是否具有特征日期类 .lbysj 与标题链接 .lbyy
            first_item = se_items[0]
            if first_item.select_one('.lbysj') and first_item.select_one('a.lbyy'):
                current = []
                for li in se_items:
                    a = li.select_one('a.lbyy') or li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                    title = a.get('title', '').strip()
                    if not title:
                        bt = a.select_one('.listxwbt') or a.select_one('p')
                        title = bt.get_text(strip=True) if bt else a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：重组 .lbysj 内的 .lbysjny (如 2026-05) 和 .lbysjr (天数，如 6)
                    date_text = ""
                    time_div = li.select_one('.lbysj')
                    if time_div:
                        day_div = time_div.select_one('.lbysjr')
                        ym_div = time_div.select_one('.lbysjny')
                        if day_div and ym_div:
                            day = day_div.get_text(strip=True)
                            year_month = ym_div.get_text(strip=True).replace('/', '-')
                            if day and year_month:
                                # 补零并合并为标准格式 YYYY-MM-DD
                                date_text = f"{year_month}-{day.zfill(2)}"

                    # 兜底提取
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 华中科大软件院 .text-listx 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增/升级：华中科大通用 div[class*="list"] li + .date2 结构 ====================
        # 泛化选择器，完美一键兼容单数 '.list'（药学院）与复数 '.lists'（艺术学院）及所有含 .date2 列的行项
        lists_items = soup.select('div[class*="list"] li, li:has(.date2)')
        if lists_items:
            # 校验该卡片列表首行是否具有特征 a 标签与右对齐日期列 .date2
            first_item = lists_items[0]
            if first_item.find('a', href=True) and first_item.select_one('.date2'):
                current = []
                for li in lists_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：.date2 类名，格式极度标准
                    date_div = li.select_one('.date2')
                    date_text = date_div.get_text(strip=True) if date_div else self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 华中科大通用 .date2 列表策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：华中科大艺术院 .lists + li + .date2 结构 ====================
        lists_items = soup.select('.lists li, div.lists li')
        if lists_items:
            # 校验该卡片列表首行是否具有特征 a 标签与右对齐日期列 .date2
            first_item = lists_items[0]
            if first_item.find('a', href=True) and first_item.select_one('.date2'):
                current = []
                for li in lists_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：.date2 类名，格式极度标准
                    date_div = li.select_one('.date2')
                    date_text = date_div.get_text(strip=True) if date_div else self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 华中科大艺术院 .lists 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：华中科大网安院 .news_list01 复合结构 ====================
        hust_cyber_items = soup.select('.news_list01 li, div.news_list01 li')
        if not hust_cyber_items:
            # 兜底：寻找同时包含日期类 .date 与正文类 .t 节点的 li 标签
            hust_cyber_items = [li for li in soup.select('li') if li.select_one('.date') and li.select_one('.t')]

        if hust_cyber_items:
            # 校验该卡片列表首行是否具有特征日期类 .date b 与标题类 .t a
            first_item = hust_cyber_items[0]
            if first_item.select_one('.date b') and first_item.select_one('.t a'):
                current = []
                for li in hust_cyber_items:
                    a = li.select_one('.t b a') or li.select_one('.t a') or li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：重组 .date 内部的 b (如 06) 和 span (如 2026-05)
                    date_text = ""
                    date_div = li.select_one('.date')
                    if date_div:
                        b_day = date_div.select_one('b')
                        span_ym = date_div.select_one('span')
                        if b_day and span_ym:
                            day = b_day.get_text(strip=True)
                            year_month = span_ym.get_text(strip=True).replace('/', '-')
                            if day and year_month:
                                # 补零合并为标准格式 YYYY-MM-DD
                                date_text = f"{year_month}-{day.zfill(2)}"

                    # 兜底提取
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 华中科大网安院 .news_list01 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：华中科大经济院 .list + h2.l1 列表结构 ====================
        hust_econ_items = soup.select('div.list li[id^="line_u"], .list li')
        if hust_econ_items:
            # 校验该卡片列表首行是否具有特征标题类 h2.l1 与日期类 .date
            first_item = hust_econ_items[0]
            if first_item.select_one('h2.l1') and first_item.select_one('.date'):
                current = []
                for li in hust_econ_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题（优先从 h2.l1 中提取）
                    h2 = a.select_one('h2.l1') or a.select_one('h2')
                    title = h2.get_text(strip=True) if h2 else a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：直接读取 .date
                    date_div = a.select_one('.date')
                    date_text = date_div.get_text(strip=True) if date_div else self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 华中科大经济院 .list 策略定位命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：华中科大管院 .list_notice 复合结构 ====================
        hust_items = soup.select('.list_notice li, div.list_notice li')
        if hust_items:
            # 校验该卡片列表首行是否具有特征日期类 span i 与标题类 h4
            first_item = hust_items[0]
            if first_item.select_one('span i') and first_item.select_one('h4'):
                current = []
                for li in hust_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题（h4 标签）
                    h4 = a.select_one('h4')
                    title = h4.get_text(strip=True) if h4 else a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：span 内部直接包含月日，以及 i 标签包含的 4 位年份
                    date_text = ""
                    span_date = a.select_one('span')
                    if span_date:
                        i_tag = span_date.select_one('i')
                        full_text = span_date.get_text(strip=True)  # 获取诸如 "09-102021" 的连读文本
                        if i_tag:
                            year = i_tag.get_text(strip=True)
                            # 从连读文本中刨除年份，净化得到月日信息
                            month_day = full_text.replace(year, "").replace('/', '-').strip()
                            if year and month_day:
                                # 补零并合并为标准格式 YYYY-MM-DD
                                date_text = f"{year}-{month_day.zfill(2)}"

                    # 兜底提取
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 华中科大管院 .list_notice 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：华中农大马院 .newTop & .newList 复合置顶结构 ====================
        hzau_marx_items = soup.select('.newList li, div.newList li')
        if hzau_marx_items or soup.select('.newTop'):
            current = []

            # 1. 优先提取顶部置顶图文项（防止遗漏最重要的头条）
            recom = soup.select_one('.newTop a.scaleimg, .newTop a')
            if recom:
                a = recom
                h3 = a.select_one('h3')
                title = h3.get_text(strip=True) if h3 else a.get('title', '').strip()
                if title and len(title) >= 6:
                    url = urljoin(base_url, a.get('href', '').strip())
                    # 日期提取：.time 内部的文本，清洗掉带有的 MORE > 标签杂质
                    time_div = a.select_one('.time')
                    date_text = ""
                    if time_div:
                        raw_time = time_div.get_text(" ", strip=True)
                        date_text = raw_time.replace('MORE >', '').replace('MORE>', '').strip()
                    if not date_text:
                        date_text = self._extract_date_from_element(a)
                    current.append({"title": title, "url": url, "date": date_text})

            # 2. 依次提取下方的常规公告列表
            if hzau_marx_items:
                for li in hzau_marx_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 标题：h3 标签
                    h3 = a.select_one('h3')
                    title = h3.get_text(strip=True) if h3 else a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期重组：提取 .time 内的 b (如 05) 与 span (如 2022-01)
                    date_text = ""
                    date_div = a.select_one('.time')
                    if date_div:
                        b = date_div.select_one('b')
                        span = date_div.select_one('span')
                        if b and span:
                            day = b.get_text(strip=True)
                            year_month = span.get_text(strip=True).replace('/', '-')
                            if day and year_month:
                                # 补零合并为标准格式 YYYY-MM-DD
                                date_text = f"{year_month}-{day.zfill(2)}"

                    # 兜底提取
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] 华中农大马院 .newList 复合置顶策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：华中农大资环院 a > i+span 复合列表结构 ====================
        hzau_list_items = soup.select('ul.clearfix li, li')
        if hzau_list_items:
            # 校验首个行内元素是否符合特征布局（a 包含 i 日期节点与内含 em 的 span 标题节点）
            first_item = hzau_list_items[0]
            a_first = first_item.find('a', href=True)
            if a_first and a_first.select_one('i') and a_first.select_one('span em'):
                current = []
                for li in hzau_list_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题并优雅剔除 em 内部多余的子弹头符号，防止标题文本被圆点污染
                    title_span = a.select_one('span')
                    if title_span:
                        em = title_span.select_one('em')
                        if em:
                            em_text = em.get_text(strip=True)
                            # 仅替换一次，并剥离首尾各种噪声符号
                            title = title_span.get_text(strip=True).replace(em_text, "", 1).strip('·•*-\t\n\r ')
                        else:
                            title = title_span.get_text(strip=True).strip('·•*-\t\n\r ')
                    else:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：a 内部开头的 i 标签
                    date_i = a.select_one('i')
                    date_text = date_i.get_text(strip=True) if date_i else self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 华中农大资环院 a > i+span 列表策略定位命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：华中农大经管院 .recomNews & .newsList 置顶复合结构 ====================
        hzau_em_items = soup.select('ul.newsList li, .newsList li')
        if hzau_em_items or soup.select('.recomNews'):
            current = []

            # 1. 优先提取顶部单条置顶大图项（防止遗漏最重要的头条）
            recom = soup.select_one('.recomNews a.scaleimg, .recomNews a')
            if recom:
                a = recom
                h5 = a.select_one('h5.TXTovehid') or a.select_one('h5')
                title = h5.get_text(strip=True) if h5 else a.get_text(strip=True)
                if title and len(title) >= 6:
                    url = urljoin(base_url, a.get('href', '').strip())
                    # 提取置顶图文的原生完整日期：直接读取 span
                    span_date = a.select_one('span')
                    date_text = span_date.get_text(strip=True) if span_date else self._extract_date_from_element(a)
                    current.append({"title": title, "url": url, "date": date_text})

            # 2. 依次提取下方的常规公告列表
            if hzau_em_items:
                for li in hzau_em_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 标题：h5.TXTovehid 标签
                    h5 = a.select_one('h5.TXTovehid') or a.select_one('h5')
                    title = h5.get_text(strip=True) if h5 else a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期重组：提取 .date 内的 em (如 15) 与 span (如 2025-05)
                    date_text = ""
                    date_div = a.select_one('.date')
                    if date_div:
                        em = date_div.select_one('em')
                        span = date_div.select_one('span')
                        if em and span:
                            day = em.get_text(strip=True)
                            year_month = span.get_text(strip=True).replace('/', '-')
                            if day and year_month:
                                # 补零合并为标准格式 YYYY-MM-DD
                                date_text = f"{year_month}-{day.zfill(2)}"

                    # 兜底提取
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] 华中农大经管院 .newsList 复合置顶策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：华中农大公管院 .TXTovehid 复合结构 ====================
        txt_items = soup.select('li:has(span.TXTovehid), li:has(.TXTovehid)')
        if not txt_items:
            # 兜底：在无法使用 :has 伪类时的通用匹配，寻找含有特征样式并包裹有 a 链接的 li 标签
            txt_items = [li for li in soup.select('li') if li.select_one('span.TXTovehid') and li.find('a', href=True)]

        if txt_items:
            # 校验首个元素是否符合特征布局（包含 span.TXTovehid 和日期 em 节点）
            first_item = txt_items[0]
            if first_item.select_one('span.TXTovehid') and first_item.select_one('em'):
                current = []
                for li in txt_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                    title = a.get('title', '').strip()
                    if not title:
                        span_tit = a.select_one('span.TXTovehid')
                        title = span_tit.get_text(strip=True) if span_tit else a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：直接读取 em 标签
                    em_tag = a.select_one('em')
                    date_text = em_tag.get_text(strip=True) if em_tag else self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 华中农大公管院 .TXTovehid 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：华中农大通用 .news-info-list 拆分日期卡片结构 ====================
        hzau_items = soup.select('.news-info-list li, li:has(.date):has(.info)')
        if not hzau_items:
            # 极限兜底：只要满足内含 .date .big 及 .info .tit 特征组合的任何 li 列表行即可激活
            hzau_items = [li for li in soup.select('li') if li.select_one('.date .big') and li.select_one('.info .tit')]

        if hzau_items:
            # 校验该卡片列表首行是否具有特征日期结构
            first_item = hzau_items[0]
            if first_item.select_one('.date') and first_item.select_one('.info'):
                current = []
                for li in hzau_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 .info .tit 类名下的纯净标题文本，丢弃下方摘要 txt 的杂质
                    tit_div = a.select_one('.info .tit') or a.select_one('.tit')
                    title = tit_div.get_text(strip=True) if tit_div else a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期重组：.date 内的 .small (如 2026) 和 .big (如 05-22)
                    date_text = ""
                    date_div = a.select_one('.date')
                    if date_div:
                        big_div = date_div.select_one('.big')
                        small_div = date_div.select_one('.small')
                        if big_div and small_div:
                            day_month = big_div.get_text(strip=True).replace('/', '-')
                            year = small_div.get_text(strip=True)
                            if day_month and year:
                                # 规范化合并为 YYYY-MM-DD
                                date_text = f"{year}-{day_month}"

                    # 兜底提取
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 华中农大通用 .news-info-list 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 确认保留：华东理工通用 .wp_article_list_table 嵌套表格结构 ====================
        ecust_items = soup.select('table.wp_article_list_table table, .wp_article_list_table table')
        if not ecust_items:
            # 兜底：匹配任意带有 wp_article_list_table 类的表格中的行级子表
            ecust_items = [table for table in soup.select('table') if
                           table.parent and table.parent.name == 'td' and table.parent.parent.parent.parent.get(
                               'class') == 'wp_article_list_table']

        if ecust_items:
            # 校验首个行内子表是否具有详情 a 标签与右对齐日期单元格
            first_item = ecust_items[0]
            if first_item.find('a', href=True) and first_item.find('td', align='right'):
                current = []
                for table in ecust_items:
                    a = table.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：优先读取右对齐的单元格 td[align="right"]，或行内最后一个单元格
                    date_td = table.find('td', align='right') or table.find_all('td')[-1]
                    date_text = date_td.get_text(strip=True) if date_td else self._extract_date_from_element(table)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 华东理工通用 .wp_article_list_table 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：华东理工化学院 .wp_article_list_table 嵌套表格结构 ====================
        ecust_items = soup.select('table.wp_article_list_table table, .wp_article_list_table table')
        if not ecust_items:
            # 兜底：匹配任意带有 wp_article_list_table 类的表格中的行
            ecust_items = [table for table in soup.select('table') if
                           table.parent and table.parent.name == 'td' and table.parent.parent.parent.parent.get(
                               'class') == 'wp_article_list_table']

        if ecust_items:
            # 校验首个行内子表格是否具有详情 a 标签与右对齐日期单元格
            first_item = ecust_items[0]
            if first_item.find('a', href=True) and first_item.find('td', align='right'):
                current = []
                for table in ecust_items:
                    a = table.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 100% 完整的 title 属性，防字符溢出截断
                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：优先读取右对齐的单元格 td[align="right"]，或行内最后一个单元格
                    date_td = table.find('td', align='right') or table.find_all('td')[-1]
                    date_text = date_td.get_text(strip=True) if date_td else self._extract_date_from_element(table)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 华东理工化学院 .wp_article_list_table 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：华东师大音乐院 .column-news-item 自引用卡片结构 ====================
        col_items = soup.select('.column-news-item, a.column-news-item')
        if col_items:
            # 校验该卡片列表是否具有特征日期类 .column-news-date 与标题类 .column-news-title
            first_item = col_items[0]
            if first_item.select_one('.column-news-title') and first_item.select_one('.column-news-date'):
                current = []
                for item in col_items:
                    # 强力兼容：判定目标行本身是否就是 a 标签，防范自引用导致 find('a') 返回空
                    a = item if item.name == 'a' else item.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题
                    title_span = item.select_one('.column-news-title')
                    title = title_span.get_text(strip=True) if title_span else a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    # 强力清除部分被前端裁切导致尾部带有的省略号：如 "..." 或 "…"
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：直接读取 .column-news-date
                    date_span = item.select_one('.column-news-date')
                    date_text = date_span.get_text(strip=True) if date_span else self._extract_date_from_element(item)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 华东师大音乐院 .column-news-item 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：华东师大法学院 .lbyy-7 复合列表结构 ====================
        law_items = soup.select('.lbyy-7 a.bt-4, .lbyy-2 a.bt-4')
        if not law_items:
            # 兜底：匹配任何包含标题样式 .bt-6 和日期样式 .time-12 且外部被 a 标签包裹的行
            law_items = [a for a in soup.select('a') if a.select_one('.bt-6') and a.select_one('.time-12')]

        if law_items:
            current = []
            for a in law_items:
                # 提取标题
                bt6 = a.select_one('.bt-6')
                title = bt6.get_text(strip=True) if bt6 else a.get('title', '').strip()
                if not title:
                    title = a.get_text(strip=True)

                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', '').strip())

                # 日期提取：直接读取 .time-12
                date_div = a.select_one('.time-12')
                date_text = date_div.get_text(strip=True) if date_div else self._extract_date_from_element(a)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] 华东师大法学院 .lbyy-7 策略定位预测命中共 {len(current)} 条")
                return current

        # ==================== 1. 新增：华东师大政国院 #post-list-sidebar 列表结构 ====================
        sidebar_items = soup.select('#post-list-sidebar .entry, .posts-sm .entry')
        if not sidebar_items:
            # 兜底：匹配任何带有 .entry 类名，且内含标题和日期的行
            sidebar_items = [div for div in soup.select('.entry') if
                             div.select_one('.entry-title') and div.select_one('.entry-meta')]

        if sidebar_items:
            # 确认其首项是否具备特征子样式
            first_item = sidebar_items[0]
            if first_item.select_one('.entry-title a') and first_item.select_one('.entry-meta'):
                current = []
                for entry in sidebar_items:
                    a = entry.select_one('.entry-title a') or entry.find('a', href=True)
                    if not a:
                        continue

                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：提取 .entry-meta ul 的第一个 li 下的文本（去除阅读数，保留完整发布时间）
                    date_text = ""
                    meta_ul = entry.select_one('.entry-meta ul')
                    if meta_ul:
                        li_tags = meta_ul.find_all('li')
                        if li_tags:
                            date_text = li_tags[0].get_text(strip=True)

                    if not date_text:
                        date_text = self._extract_date_from_element(entry)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 华东师大政国院 #post-list-sidebar 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：华东师大地科院 .news_list.list2 + .news_box 复合结构 ====================
        ecnu_geo_items = soup.select('ul.news_list.list2 li, .news_list.list2 li')
        if ecnu_geo_items:
            # 校验该卡片列表首行是否具有特有的 news_box 卡片与 news_date 样式（防止与中文系、传播系冲突）
            first_item = ecnu_geo_items[0]
            if first_item.select_one('a.news_box') and first_item.select_one('.news_date'):
                current = []
                for li in ecnu_geo_items:
                    a = li.select_one('a.news_box') or li.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题
                    title_div = a.select_one('.news_title')
                    title = title_div.get_text(strip=True) if title_div else a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回', '了解详情']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：重组 .news_year (如 2026-02) 和 .news_day (如 11)
                    date_text = ""
                    date_div = a.select_one('.news_date')
                    if date_div:
                        day_div = date_div.select_one('.news_day')
                        year_div = date_div.select_one('.news_year')
                        if day_div and year_div:
                            day = day_div.get_text(strip=True)
                            year_month = year_div.get_text(strip=True).replace('/', '-')
                            if day and year_month:
                                # 补零合并为标准格式 YYYY-MM-DD
                                date_text = f"{year_month}-{day.zfill(2)}"

                    # 兜底提取
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 华东师大地科院 .news_box 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：华东师大传播学院 .news_list.list2 结构 ====================
        news_list2_items = soup.select('ul.news_list.list2 li, .news_list.list2 li')
        if news_list2_items:
            # 校验该卡片列表是否具有特征日期类 .news_meta 与标题类 .news_title
            first_item = news_list2_items[0]
            if first_item.select_one('.news_title a') and first_item.select_one('.news_meta'):
                current = []
                for li in news_list2_items:
                    a = li.select_one('.news_title a') or li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：span.news_meta 中的标准时间
                    date_span = li.select_one('.news_meta')
                    date_text = date_span.get_text(strip=True) if date_span else self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 华东师大传播院 .news_list.list2 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：华东师大中文系 .ac-xul 复合结构 ====================
        ecnu_items = soup.select('ul.ac-xul li, .ac-xul li')
        if ecnu_items:
            # 校验该卡片列表是否具有特征日期类 .li-date 与标题内容类 .li-dtt
            first_item = ecnu_items[0]
            if first_item.select_one('.li-date') and first_item.select_one('.li-dtt'):
                current = []
                for li in ecnu_items:
                    # 过滤获取真正的公告详情链接（避开 class 含有 a1 的 "查看详情" 按钮）
                    valid_a = None
                    a_tags = li.find_all('a', href=True)
                    for a in a_tags:
                        a_classes = a.get('class', [])
                        if not a_classes or 'a1' not in a_classes:
                            valid_a = a
                            break

                    if not valid_a:
                        continue

                    # 优先获取 a 标签 100% 完整的 title 属性，防字符溢出截断
                    title = valid_a.get('title', '').strip()
                    if not title:
                        title = valid_a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回', '查看详情']:
                        continue

                    url = urljoin(base_url, valid_a.get('href', '').strip())

                    # 日期提取：重组 .li-date 内部的 span (如 2024-06) 和 p (如 28)
                    date_text = ""
                    date_div = li.select_one('.li-date')
                    if date_div:
                        p_day = date_div.select_one('p')
                        span_ym = date_div.select_one('span')
                        if p_day and span_ym:
                            day = p_day.get_text(strip=True)
                            year_month = span_ym.get_text(strip=True).replace('/', '-')
                            if day and year_month:
                                # 补零合并为标准格式 YYYY-MM-DD
                                date_text = f"{year_month}-{day.zfill(2)}"

                    # 兜底提取
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 华东师大中文系 .ac-xul 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：华东交大继教院 .list-content-item 复合结构 ====================
        item_boxes = soup.select('.list-content-item, div.list-content-item')
        if item_boxes:
            # 校验该卡片列表行是否具备独立的 table 容器以及 white-space 特征日期
            first_item = item_boxes[0]
            if first_item.find('a', href=True) and first_item.select_one(
                    'div[style*="white-space:nowrap"], div[style*="nowrap"]'):
                current = []
                for box in item_boxes:
                    a = box.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：寻找具有 white-space:nowrap 样式的 div，或在 td 内部兜底查找
                    date_div = box.select_one('div[style*="white-space:nowrap"]') or box.select_one(
                        'div[style*="nowrap"]')
                    if not date_div:
                        tds = box.find_all('td')
                        if len(tds) >= 2:
                            date_div = tds[-1].select_one('div') or tds[-1]

                    date_text = date_div.get_text(strip=True) if date_div else self._extract_date_from_element(box)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 华东交大继教院 .list-content-item 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：华东交大材能院 .list_plaintext 列表结构 ====================
        ecjtu_items = soup.select('.list_plaintext a')
        if ecjtu_items:
            # 校验该卡片列表是否具有特征日期类 time 与标题类 h3
            first_item = ecjtu_items[0]
            if first_item.select_one('h3') and first_item.select_one('time'):
                current = []
                for a in ecjtu_items:
                    # 精妙避噪：优先获取 a 标签 100% 完整的 title 属性，彻底隔绝 <i> 里的图标实体字符
                    title = a.get('title', '').strip()
                    if not title:
                        h3 = a.select_one('h3')
                        title = h3.get_text(strip=True) if h3 else a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：time 标签
                    time_tag = a.select_one('time')
                    date_text = time_tag.get_text(strip=True) if time_tag else self._extract_date_from_element(a)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 华东交大材能院 .list_plaintext 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：北方工业大学集成电路院 .text-list 复合结构 ====================
        ncut_ic_items = soup.select('.text-list li, div.text-list li')
        if ncut_ic_items:
            # 校验该卡片列表是否具有特征日期结构 .time time 与标题类 .titline
            first_item = ncut_ic_items[0]
            if first_item.select_one('.time time') and first_item.select_one('.titline'):
                current = []
                for li in ncut_ic_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 强力技巧：优先获取 a 标签上未被前端省略号截断的 title 完整属性值
                    title = a.get('title', '').strip()
                    if not title:
                        h3 = a.select_one('.titline') or a.select_one('h3')
                        title = h3.get_text(strip=True) if h3 else a.get_text(strip=True)

                    # 强力清除部分被前端裁切导致尾部带有的省略号：如 "..." 或 "…"
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：重组 time 内的 span (如 14) 与其余文本 (如 2026-04)
                    date_text = ""
                    time_tag = a.select_one('.time time') or a.select_one('time')
                    if time_tag:
                        span = time_tag.select_one('span')
                        full_text = time_tag.get_text(strip=True)  # 获取诸如 "142026-04" 的连读文本
                        if span:
                            day = span.get_text(strip=True)
                            # 从连读文本中刨除天数，净化得到年月信息
                            year_month = full_text.replace(day, "").replace('/', '-').strip()
                            if day and year_month:
                                # 补零合并为标准格式 YYYY-MM-DD
                                date_text = f"{year_month}-{day.zfill(2)}"

                    # 兜底提取
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 北方工业大学集成电路院 .text-list 策略定位命中 → {len(current)} 条")
                    return current

        # ==================== 1. 确认保留：北邮 / 北工大通用 ul.ss 列表结构 ====================
        ss_items = soup.select('ul.ss li')
        if ss_items:
            # 校验该卡片列表是否具有特征日期类 span 与详情 a
            first_item = ss_items[0]
            if first_item.find('a', href=True) and first_item.select_one('span'):
                current = []
                for li in ss_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 强力技巧：优先获取 a 标签上未被前端省略号截断的 title 完整属性值
                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：span（优先读取带 19% 宽度的 span 样式，或首个子 span）
                    span_date = li.select_one('span[style*="width:19%"]') or li.select_one(
                        'span[style*="19%"]') or li.select_one('span')
                    date_text = span_date.get_text(strip=True) if span_date else self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 通用 .ss 极简列表策略定位命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：北邮人工智能学院 ul.ss 列表结构 ====================
        ss_items = soup.select('ul.ss li')
        if ss_items:
            # 校验该卡片列表是否具有特征日期类 span 与详情 a
            first_item = ss_items[0]
            if first_item.find('a', href=True) and first_item.select_one('span'):
                current = []
                for li in ss_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 强力技巧：优先获取 a 标签上未被前端省略号截断的 title 完整属性值
                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：span（优先读取带 19% 宽度的 span 样式，或首个子 span）
                    span_date = li.select_one('span[style*="width:19%"]') or li.select_one(
                        'span[style*="19%"]') or li.select_one('span')
                    date_text = span_date.get_text(strip=True) if span_date else self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 北邮人工智能院 .ss 极简列表策略定位命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：北语语科院 .nr_both + span.fbdate 结构 ====================
        nr_both_items = soup.select('.nr_both a, div.nr_both a')
        if nr_both_items:
            # 校验该卡片列表是否具有特征日期类 .fbdate 与标题类 .tit3
            first_item = nr_both_items[0]
            if first_item.select_one('p.tit3') and first_item.select_one('.fbdate'):
                current = []
                for a in nr_both_items:
                    # 提取标题
                    p_tit = a.select_one('p.tit3') or a.select_one('.tit3')
                    title = p_tit.get_text(strip=True) if p_tit else a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：span.fbdate
                    date_span = a.select_one('span.fbdate') or a.select_one('.fbdate')
                    date_text = date_span.get_text(strip=True) if date_span else self._extract_date_from_element(a)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 北语语科院 .nr_both 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：北语汉学学院 .ej_list1 复合结构 ====================
        ej_items = soup.select('ul.ej_list1 li, .ej_list1 li')
        if ej_items:
            # 校验该卡片列表是否具有特征日期类 .list_ljgd 与标题类 .list_tijzy
            first_item = ej_items[0]
            if first_item.select_one('.list_tijzy') and first_item.select_one('.list_ljgd'):
                current = []
                for li in ej_items:
                    a_title = li.select_one('.list_tijzy a') or li.find('a', href=True)
                    if not a_title:
                        continue

                    # 优先获取 a 标签 100% 完整的 title 属性，防止文字被前端截断
                    title = a_title.get('title', '').strip()
                    if not title:
                        title = a_title.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a_title.get('href', '').strip())

                    # 日期提取：直接读取 .list_ljgd 节点文本
                    date_div = li.select_one('.list_ljgd')
                    date_text = date_div.get_text(strip=True) if date_div else self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 北语汉学学院 .ej_list1 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：北语国传学院 .xynewsbox + .newdate 结构 ====================
        xynews_items = soup.select('.xynewsbox div.news, .xynewsbox div.newsbox')
        if not xynews_items:
            # 兜底：匹配任意容器下同时包含 .newstitle a 和 .newdate 的行
            xynews_items = [div for div in soup.select('div') if
                            div.select_one('.newstitle a') and div.select_one('.newdate')]

        if xynews_items:
            # 确认其是否具备该特征子样式
            first_item = xynews_items[0]
            if first_item.select_one('.newstitle a') and first_item.select_one('.newdate'):
                current = []
                for item in xynews_items:
                    a = item.select_one('.newstitle a') or item.find('a', href=True)
                    if not a:
                        continue

                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：直接读取 .newdate
                    date_div = item.select_one('.newdate')
                    date_text = date_div.get_text(strip=True) if date_div else self._extract_date_from_element(item)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 北语国传学院 .xynewsbox 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：北语商学院 .tzgg + h3 a 复合结构 ====================
        blcu_items = soup.select('.tzgg li[id^="line_u"], .tzgg li')
        if blcu_items:
            # 校验该卡片列表是否具有特征日期类 span 与标题类 h3 a
            first_item = blcu_items[0]
            if first_item.select_one('h3 a') and first_item.select_one('span'):
                current = []
                for li in blcu_items:
                    a = li.select_one('h3 a') or li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：直接读取 li 第一个子 span 的值
                    date_span = li.select_one('span')
                    date_text = date_span.get_text(strip=True) if date_span else self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 北语商学院 .tzgg 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：北航集成电路学院 .lt18 复合拆分日期结构 ====================
        buaa_ic_items = soup.select('ul.lt18 li[id^="line_u"], .lt18 li')
        if buaa_ic_items:
            # 校验该卡片列表是否具有特征日期结构 .time.flex 与标题类 h4 标签
            first_item = buaa_ic_items[0]
            if first_item.select_one('.time.flex') and first_item.select_one('h4'):
                current = []
                for li in buaa_ic_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题
                    h4 = a.select_one('h4')
                    title = h4.get_text(strip=True) if h4 else a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：重组 .time 内的 small (如 / 2025) 和 big (如 09-12)
                    date_text = ""
                    time_div = a.select_one('.time')
                    if time_div:
                        big = time_div.select_one('big')
                        small = time_div.select_one('small')
                        if big and small:
                            day_month = big.get_text(strip=True)
                            year = small.get_text(strip=True).replace('/', '').strip()
                            if day_month and year:
                                # 拼合为标准 YYYY-MM-DD
                                date_text = f"{year}-{day_month.replace('/', '-')}"

                    # 兜底提取
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 北航集成电路院 .lt18 策略定位命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：头部主导航/下拉菜单 .topNav 复合结构 ====================
        menu_items = soup.select('.topNav .sub-nav-h a, .navs .sub-nav-h a, .sub-nav-h a')
        if not menu_items:
            # 兜底：顶级主导航栏
            menu_items = soup.select('.topNav .navs a, .navs a')

        if menu_items:
            current = []
            # 严格排除顶级无意义分类、操作性文案（其余均为高价值二级栏目，如：通知公告）
            exclusions = [
                '首页', '返回', '网上投稿', '联系我们', '综合新闻', '学院概况',
                '师资队伍', '科学研究', '人才培养', '党群建设', '教材基地', '办事指南'
            ]
            for a in menu_items:
                # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                title = a.get('title', '').strip()
                if not title:
                    title = a.get_text(strip=True)

                # 将标题清洗干净后做判定
                title_cleaned = re.sub(r'[\s　]+', '', title)
                if len(title_cleaned) < 4 or title_cleaned in exclusions:
                    continue

                url = urljoin(base_url, a.get('href', '').strip())

                # 导航菜单无物理日期
                date_text = self._extract_date_from_element(a)

                current.append({"title": title_cleaned, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] 头部主导航 .topNav 策略定位命中共 {len(current)} 条")
                return current

        # ==================== 新增：text-ldata + text-linfo 结构 ====================
        ldata_items = soup.select('ul li:has(.text-ldata)')
        if ldata_items:
            current = []
            for li in ldata_items:
                a = li.select_one('a')
                if not a:
                    continue

                # 标题（优先 h3）
                h3 = a.select_one('h3')
                title = h3.get_text(strip=True) if h3 else a.get('title', '') or a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期
                date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 2:
                self.log.emit(f"[成功] text-ldata + text-linfo 策略命中 → {len(current)} 条")
                return current

        # ==================== 新增：d-newsU5 结构 ====================
        dnews_items = soup.select('ul.d-newsU5 li')
        if dnews_items:
            current = []
            for li in dnews_items:
                a = li.select_one('a.inner, a')
                if not a:
                    continue

                # 标题
                h4 = a.select_one('h4.tit')
                title = h4.get_text(strip=True) if h4 else a.get('title', '') or a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期
                date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 2:
                self.log.emit(f"[成功] ul.d-newsU5 策略命中 → {len(current)} 条")
                return current

        # ==================== 新增：n_list + line_u9_ 结构 ====================
        line_items = soup.select('ul.n_list li[id^="line_u"], li[id^="line_u9_"]')
        if line_items:
            current = []
            for li in line_items:
                a = li.find('a', href=True)
                if not a:
                    continue

                title = a.get('title', '') or a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期：直接跟在 </a> 后面的文本
                date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 2:
                self.log.emit(f"[成功] n_list + line_u9_ 策略命中 → {len(current)} 条")
                return current

        # ==================== 新增：ul.flex + div.date 结构 ====================
        flex_items = soup.select('ul.flex li[aos="fade-up"], ul.flex li')
        if flex_items:
            current = []
            for li in flex_items:
                a = li.select_one('a.flex.wl, a')
                if not a:
                    continue

                # 标题
                h3 = a.select_one('h3.l1.txt1, h3')
                title = h3.get_text(strip=True) if h3 else a.get('title', '') or a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期
                date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 2:
                self.log.emit(f"[成功] ul.flex + div.date 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：line_u12_ 结构（最常见的一种） ====================
        line_items = soup.select('li[id^="line_u"]')
        if line_items:
            current = []
            for li in line_items:
                a = li.find('a', href=True)
                if not a:
                    continue

                # 优先使用 title 属性（更完整）
                title = a.get('title', '').strip()
                if not title:
                    title = a.get_text(strip=True)

                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期：紧跟 a 标签的 <span>
                date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 1:  # 这类结构通常数量不多，1条也接受
                self.log.emit(f"[成功] li#line_uXX 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：侧边栏/分类导航 .d-sNavU1 结构 ====================
        nav_items = soup.select('.d-sNavU1 li, ul.d-sNavU1 li')
        if nav_items:
            # 校验该卡片列表是否具有特征类 a.tit
            first_item = nav_items[0]
            if first_item.select_one('a.tit') or first_item.find('a', href=True):
                current = []
                for li in nav_items:
                    a = li.select_one('a.tit') or li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 100% 完整的 title 属性
                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 侧边栏导航通常无日期
                    date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 侧边栏导航 .d-sNavU1 策略定位命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：dl#line_u8_ 结构 ====================
        dl_items = soup.select('dl[id^="line_u8_"]')
        if dl_items:
            current = []
            for dl in dl_items:
                a = dl.select_one('dd a, a')
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 无独立日期，从标题提取年份
                date_text = self._extract_date_from_element(dl)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 2:
                self.log.emit(f"[成功] dl#line_u8_ 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：北科大高精尖学院 .gpArticleDate 复合拆分日期结构 ====================
        gp_adv_items = soup.select('ul[class*="gp-avg"] li, .wrap_content li')
        if gp_adv_items:
            # 校验该卡片列表是否具有特征多层日期结构与标题类 .gpArticleTitle
            first_item = gp_adv_items[0]
            if first_item.select_one('.gpArticleDate') and first_item.select_one('.gpArticleTitle'):
                current = []
                for li in gp_adv_items:
                    a = li.select_one('a.gpTextArea') or li.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题
                    title_div = a.select_one('.gpArticleTitle')
                    title = title_div.get_text(strip=True) if title_div else a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期重组：分别提取 .year (如 2026) + .month (含中文字"05 月") + .day (如 18)
                    date_text = ""
                    time_div = a.select_one('.gpArticleDate')
                    if time_div:
                        year_div = time_div.select_one('.year')
                        month_div = time_div.select_one('.month')
                        day_div = time_div.select_one('.day')
                        if year_div and month_div and day_div:
                            year = year_div.get_text(strip=True)
                            month = month_div.get_text(strip=True).replace('月', '').strip()
                            day = day_div.get_text(strip=True)
                            # 确定转换出的年月日数据合法再进行合流
                            if year.isdigit() and month.isdigit() and day.isdigit():
                                date_text = f"{year}-{month.zfill(2)}-{day.zfill(2)}"

                    # 兜底提取
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 北科大高精尖院 .gpArticleDate 复合策略定位命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：北科大马院 .newlist 复合结构 ====================
        ustb_marx_items = soup.select('ul.newlist li, .newlist li')
        if ustb_marx_items:
            # 校验该卡片列表是否具有特征日期结构与特征标题节点
            first_item = ustb_marx_items[0]
            if first_item.select_one('.focus-time') and first_item.select_one('.tex h3 a'):
                current = []
                for li in ustb_marx_items:
                    a = li.select_one('.tex h3 a') or li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：重组 .focus-time 内的 h5 (如 24) 和 span (如 2026-04)
                    date_text = ""
                    time_div = li.select_one('.focus-time')
                    if time_div:
                        day_h5 = time_div.select_one('h5')
                        ym_span = time_div.select_one('span')
                        if day_h5 and ym_span:
                            day = day_h5.get_text(strip=True)
                            year_month = ym_span.get_text(strip=True).replace('/', '-')
                            if day and year_month:
                                # 补零合并为标准格式 YYYY-MM-DD
                                date_text = f"{year_month}-{day.zfill(2)}"

                    # 兜底提取
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 北科大马院 .newlist 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：北科大未来城市学院 .gpTextArea 复合结构 ====================
        gp_items = soup.select('li a.gpTextArea, a.gpTextArea')
        if gp_items:
            # 校验该卡片列表是否具有特征日期类 .gpArticleDate 与标题类 .gpArticleTitle
            first_item = gp_items[0]
            if first_item.select_one('.gpArticleTitle') and first_item.select_one('.gpArticleDate'):
                current = []
                for a in gp_items:
                    # 提取标题
                    title_p = a.select_one('.gpArticleTitle')
                    title = title_p.get_text(strip=True) if title_p else a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：直接读取 .gpArticleDate
                    date_span = a.select_one('.gpArticleDate')
                    date_text = date_span.get_text(strip=True) if date_span else self._extract_date_from_element(a)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 北科大未来城市院 .gpTextArea 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：specialDate_fontBox 结构 ====================
        special_items = soup.select('div.specialDate_fontBox')
        if special_items:
            current = []
            for box in special_items:
                a = box.select_one('a.specialTime_a, a')
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期提取（specialTime）
                date_text = self._extract_date_from_element(box)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 2:
                self.log.emit(f"[成功] specialDate_fontBox 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：北服研究生院 .lzd-blog + .lzd-blog-headline 复合结构 ====================
        bift_items = soup.select('.lzd-blog .row, div.lzd-blog div.row')
        if bift_items:
            # 过滤并提取真正含有日期类 lzd-blog-date 和标题类的公告行
            valid_items = [row for row in bift_items if
                           row.select_one('.lzd-blog-date') and row.select_one('.lzd-blog-headline')]

            if valid_items:
                current = []
                for row in valid_items:
                    a = row.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题（优先从 .lzd-blog-headline 获取）
                    hl = row.select_one('.lzd-blog-headline')
                    title = hl.get_text(strip=True) if hl else a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    # 强力清除部分被前端裁切导致尾部带有的省略号：如 "..." 或 "…"
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：直接读取标配的 .lzd-blog-date 属性
                    date_h4 = row.select_one('.lzd-blog-date')
                    date_text = date_h4.get_text(strip=True) if date_h4 else self._extract_date_from_element(row)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 北服研究生院 .lzd-blog 策略定位命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：北师大马院 .essay-list + .list-title 结构 ====================
        essay_items = soup.select('ul.essay-list a, .essay-list a')
        if essay_items:
            # 校验该卡片列表是否具有特征日期类 .list-time 与标题类 .list-title
            first_item = essay_items[0]
            if first_item.select_one('.list-title') and first_item.select_one('.list-time'):
                current = []
                for a in essay_items:
                    # 提取标题
                    title_span = a.select_one('.list-title')
                    title = title_span.get_text(strip=True) if title_span else a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：.list-time 中的日期直接即为标准形式
                    date_span = a.select_one('.list-time')
                    date_text = date_span.get_text(strip=True) if date_span else self._extract_date_from_element(a)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 北师大马院 .essay-list 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：北师大珠海校区 .news-content + .content-warp 结构 ====================
        zh_items = soup.select('.news-content .content-warp, .news-content div.pointer')
        if not zh_items:
            # 兜底：匹配任意带有 .content-warp 类名，且内含标题和日期的行
            zh_items = [div for div in soup.select('.content-warp') if div.select_one('h2 a') and div.select_one('p')]

        if zh_items:
            current = []
            for div in zh_items:
                # 兼容外层 pointer 盒子或内层 content-warp 盒子
                wrap = div.select_one('.content-warp') if 'content-warp' not in div.get('class', []) else div
                if not wrap:
                    continue

                a = wrap.select_one('h2 a') or wrap.find('a', href=True)
                if not a:
                    continue

                title = a.get('title', '').strip()
                if not title:
                    title = a.get_text(strip=True)

                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', '').strip())

                # 日期提取：提取第一个 p 标签内的文本（去除图片，留下纯文本如 "04-11"）
                date_text = ""
                p_tag = wrap.select_one('p')
                if p_tag:
                    date_text = p_tag.get_text(strip=True)
                else:
                    date_text = self._extract_date_from_element(wrap)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] 北师大珠海校区 .content-warp 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：北师大社会学院 .nynewlist 列表结构 ====================
        nynew_items = soup.select('.nynewlist li, div.nynewlist li')
        if nynew_items:
            # 校验该卡片列表是否具有特征日期类 .time 与标题类 .title
            first_item = nynew_items[0]
            if first_item.select_one('.title') and first_item.select_one('.time'):
                current = []
                for li in nynew_items:
                    a = li.select_one('.title a') or li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：.time
                    date_div = li.select_one('.time')
                    date_text = date_div.get_text(strip=True) if date_div else self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 北师大社会学院 .nynewlist 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：北师大研招网 .boxlist + .pull-right 结构 ====================
        boxlist_items = soup.select('.boxlist li, div.boxlist li')
        if boxlist_items:
            # 校验该列表行是否具备特征分类类 counr 和右浮动日期列
            first_item = boxlist_items[0]
            if first_item.select_one('.counr') and first_item.select_one('.pull-right'):
                current = []
                for li in boxlist_items:
                    # 过滤获取真正的公告详情链接（避开 counr 类链接）
                    valid_a = None
                    a_tags = li.find_all('a', href=True)
                    for a in a_tags:
                        a_classes = a.get('class', [])
                        # 如果没有类名或类名不含 'counr'，判定为我们要找的详情页
                        if not a_classes or 'counr' not in a_classes:
                            valid_a = a
                            break

                    if not valid_a:
                        continue

                    # 提取标题
                    title = valid_a.get('title', '').strip()
                    if not title:
                        title = valid_a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, valid_a.get('href', '').strip())

                    # 日期提取：span.pull-right
                    date_span = li.select_one('span.pull-right, .pull-right')
                    date_text = date_span.get_text(strip=True) if date_span else self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 北师大研招网 .boxlist 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：北师大未来设计学院 .newslist + a.news-warp 结构 ====================
        fds_items = soup.select('.newslist a.news-warp, .newslist a')
        if fds_items:
            # 校验该卡片列表是否具有特征日期类 .title-en 与标题类 .news-title
            first_item = fds_items[0]
            if first_item.select_one('.news-title') and first_item.select_one('.title-en'):
                current = []
                for a in fds_items:
                    # 提取标题
                    h3 = a.select_one('.news-title')
                    title = h3.get_text(strip=True) if h3 else a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：.title-en 内部的日期格式本身即标准
                    date_span = a.select_one('.title-en')
                    date_text = date_span.get_text(strip=True) if date_span else self._extract_date_from_element(a)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 北师大未来设计院 .newslist 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：北师大新传院 .listbox + .timebox .time 复合结构 ====================
        bnu_jc_items = soup.select('.tzgg .listbox a.item, .listbox a.item')
        if bnu_jc_items:
            # 校验该卡片列表是否具有特征日期类 timebox 与标题类 text h3
            first_item = bnu_jc_items[0]
            if first_item.select_one('.timebox .time') and first_item.select_one('.text h3'):
                current = []
                for a in bnu_jc_items:
                    # 提取标题
                    h3 = a.select_one('.text h3')
                    title = h3.get_text(strip=True) if h3 else a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：.time 内的 span (如 2026/05) 替换为 - 并拼接 em (如 14)
                    date_text = ""
                    time_div = a.select_one('.timebox .time')
                    if time_div:
                        em = time_div.select_one('em')
                        span = time_div.select_one('span')
                        if em and span:
                            day = em.get_text(strip=True)
                            year_month = span.get_text(strip=True).replace('/', '-')
                            if day and year_month:
                                # 补零合并为标准格式 YYYY-MM-DD
                                date_text = f"{year_month}-{day.zfill(2)}"

                    # 兜底提取
                    if not date_text:
                        date_text = self._extract_date_from_element(a)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 北师大新传院 .listbox 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：北师大文学院 section.TextList + .data 复合结构 ====================
        bnu_cl_items = soup.select('section.TextList li a.flex, .TextList li a.flex')
        if not bnu_cl_items:
            # 兜底：匹配任意列表中，日期使用 div.data 下的 b 和 i 表示的结构
            bnu_cl_items = [a for a in soup.select('li a.flex') if a.select_one('.data i') and a.select_one('.data b')]

        if bnu_cl_items:
            current = []
            for a in bnu_cl_items:
                # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                title = a.get('title', '').strip()
                if not title:
                    p_line = a.select_one('p.line1') or a.select_one('.line1')
                    title = p_line.get_text(strip=True) if p_line else a.get_text(strip=True)

                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', '').strip())

                # 日期重组：.data 内部的 b 为年份，i 为月-日
                date_text = ""
                data_div = a.select_one('.data')
                if data_div:
                    day_month = data_div.select_one('i')
                    year_b = data_div.select_one('b')
                    if day_month and year_b:
                        dm = day_month.get_text(strip=True)  # "05-08"
                        year = year_b.get_text(strip=True)  # "2026"
                        if dm and year:
                            # 标准化拼合为 YYYY-MM-DD
                            date_text = f"{year}-{dm}"

                # 兜底提取
                if not date_text:
                    date_text = self._extract_date_from_element(a)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] 北师大文学院 .TextList 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：北师大外文学院 .listconrn 复合结构 ====================
        bnu_fl_items = soup.select('ul.listconrn a, .listconrn a')
        if bnu_fl_items:
            # 校验该卡片列表是否具有特征日期类 span.time 与标题类 .listconrn-rb
            first_item = bnu_fl_items[0]
            if first_item.select_one('.listconrn-rb') and first_item.select_one('span.time'):
                current = []
                for a in bnu_fl_items:
                    # 优先获取 100% 完整的属性标题，防前端文本截断
                    rb_div = a.select_one('.listconrn-rb')
                    title = rb_div.get('title', '').strip() if rb_div else ''
                    if not title:
                        title = rb_div.get_text(strip=True) if rb_div else a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：span.time
                    date_span = a.select_one('span.time, .time')
                    date_text = date_span.get_text(strip=True) if date_span else self._extract_date_from_element(a)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 北师大外文院 .listconrn 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：北师大人工智能学院 .tz-date + .tz-txt 复合结构 ====================
        bnu_ai_items = soup.select('.tj-intro li, .tz li')
        if bnu_ai_items:
            # 校验该卡片列表是否具有特征日期结构与特征标题节点
            first_item = bnu_ai_items[0]
            if first_item.select_one('.tz-date') and first_item.select_one('.tz-txt'):
                current = []
                for li in bnu_ai_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题（位于 .tz-txt p 中）
                    txt_p = a.select_one('.tz-txt p') or a.select_one('.tz-txt')
                    title = txt_p.get_text(strip=True) if txt_p else a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：.tz-date 中的 i (如 2026.05) 并将 '.' 替换为 '-' 拼装上 span (如 21)
                    date_text = ""
                    date_div = a.select_one('.tz-date')
                    if date_div:
                        day_span = date_div.select_one('span')
                        ym_i = date_div.select_one('i')
                        if day_span and ym_i:
                            day = day_span.get_text(strip=True)
                            year_month = ym_i.get_text(strip=True).replace('.', '-')
                            if day and year_month:
                                # 补零合并为标准格式 YYYY-MM-DD
                                date_text = f"{year_month}-{day.zfill(2)}"

                    # 兜底提取
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 北师大人工智能院 .tz-date + .tz-txt 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：北师大中华文化研究院 .news-info-list 结构 ====================
        bnu_items = soup.select('ul.news-info-list a, .news-info-list a')
        if bnu_items:
            # 校验该卡片列表是否具有特征日期类 .news-info 与标题类 .news-title
            first_item = bnu_items[0]
            if first_item.select_one('.news-title') and first_item.select_one('.news-info'):
                current = []
                for a in bnu_items:
                    # 提取标题
                    title_div = a.select_one('.news-title')
                    title = title_div.get_text(strip=True) if title_div else a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：.news-info 内部的中文日期
                    date_div = a.select_one('.news-info')
                    date_text = date_div.get_text(strip=True) if date_div else self._extract_date_from_element(a)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 北师大中华文化院 .news-info-list 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：北工大化生学院 line_u + i + h2 复合结构 ====================
        bjut_items = soup.select('ul li[id^="line_u"]')
        if bjut_items:
            # 校验该列表行是否具备独立的 span i 日期结构和标题 h2 标签
            first_item = bjut_items[0]
            if first_item.select_one('span i') and first_item.select_one('a h2'):
                current = []
                for li in bjut_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题（位于 h2 中）
                    h2 = a.select_one('h2')
                    title = h2.get_text(strip=True) if h2 else a.get_text(strip=True)
                    # 强力清除部分被前端裁切导致尾部带有的省略号：如 "..." 或 "…"
                    title = re.sub(r'[…\s\.]+$', '', title).strip()

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：重组 span 内的 i (如 21) 与其余文本 (如 2026-05)
                    date_text = ""
                    span_date = li.select_one('span')
                    if span_date:
                        i_tag = span_date.select_one('i')
                        full_span_text = span_date.get_text(strip=True)  # 如 "212026-05"
                        if i_tag:
                            day = i_tag.get_text(strip=True)
                            # 剔除天数和斜杠，只留下年月信息
                            year_month = full_span_text.replace(day, "").replace('/', '').strip()
                            if day and year_month:
                                # 补零合并为标准格式 YYYY-MM-DD
                                date_text = f"{year_month}-{day.zfill(2)}"

                    # 兜底提取
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 北工大化生院 line_u 策略定位命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：ul.block-list58 ====================
        block_items = soup.select('ul.block-list58 li')
        if block_items:
            current = []
            for li in block_items:
                a = li.find('a', href=True)
                if not a:
                    continue

                # 标题
                title_p = a.select_one('p.gpArticleTitle')
                title = title_p.get_text(strip=True) if title_p else a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期（标题中提取年份）
                date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 2:
                self.log.emit(f"[成功] ul.block-list58 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：北交大经管学院 .all_list 列表结构 ====================
        all_list_items = soup.select('ul.all_list p, .all_list p')
        if all_list_items:
            # 校验行内是否具备特征日期类 time_all 且包含至少两个链接
            first_item = all_list_items[0]
            if first_item.select_one('span.time_all') and len(first_item.find_all('a')) >= 2:
                current = []
                for p in all_list_items:
                    # 过滤获取真正的公告详情链接（避开 lists- 面包屑链接）
                    valid_a = None
                    a_tags = p.find_all('a', href=True)
                    for a in a_tags:
                        href = a.get('href', '')
                        if 'lists-' not in href:
                            valid_a = a
                            break

                    if not valid_a:
                        continue

                    # 提取标题
                    title = valid_a.get('title', '').strip()
                    if not title:
                        title = valid_a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, valid_a.get('href', '').strip())

                    # 日期提取：span.time_all
                    date_span = p.select_one('span.time_all, .time_all')
                    date_text = ""
                    if date_span:
                        # 点号替换为连字符，如 "2026.04.20" -> "2026-04-20"
                        date_text = date_span.get_text(strip=True).replace('.', '-')
                    else:
                        date_text = self._extract_date_from_element(p)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 北交大经管院 .all_list 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：北交大博士招生网 .list01 复合列表结构 ====================
        list01_items = soup.select('ul.list01 li, li:has(.timeListPartnerTitle)')
        if not list01_items:
            # 兜底：寻找同时包含标题样式 .timeListPartnerTitle 与日期样式 .subListTime 的行项
            list01_items = [li for li in soup.select('li') if
                            li.select_one('.timeListPartnerTitle') and li.select_one('.subListTime')]

        if list01_items:
            # 校验首个行元素是否符合特征排版
            first_item = list01_items[0]
            if first_item.select_one('.timeListPartnerTitle') and first_item.select_one('.subListTime'):
                current = []
                for li in list01_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题
                    title_p = a.select_one('.timeListPartnerTitle')
                    title = title_p.get_text(strip=True) if title_p else a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：直接读取 .subListTime
                    date_div = a.select_one('.subListTime')
                    date_text = date_div.get_text(strip=True) if date_div else self._extract_date_from_element(a)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 北交大博士招生网 .list01 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：北交大电信学院 .sub_list_li 列表结构 ====================
        bjtu_seie_items = soup.select('ul.sub_list li.sub_list_li, li.sub_list_li')
        if bjtu_seie_items:
            # 确认其是否具备该特征子样式：.sub_list_title 与 .sub_list_date
            first_item = bjtu_seie_items[0]
            if first_item.select_one('.sub_list_title') and first_item.select_one('.sub_list_date'):
                current = []
                for li in bjtu_seie_items:
                    a = li.select_one('.sub_list_title a') or li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：.sub_list_date 内部的点号分隔日期 "YYYY.MM.DD"
                    date_div = li.select_one('.sub_list_date')
                    date_text = ""
                    if date_div:
                        # 点号替换为连字符，如 "2026.03.22" -> "2026-03-22"
                        date_text = date_div.get_text(strip=True).replace('.', '-')
                    else:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 北交大电信院 .sub_list_li 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：北交大卓越工程师学院 .subTongzhiTextList 结构 ====================
        tongzhi_items = soup.select('.subTongzhiTextList a, div.subTongzhiTextList a')
        if tongzhi_items:
            # 校验是否具有其英文拼写特异的标题样式与日期样式
            first_item = tongzhi_items[0]
            if first_item.select_one('.subTongzhiTextItemTittle') and first_item.select_one('.subTongzhiTextItemDate'):
                current = []
                for a in tongzhi_items:
                    # 提取标题（精准匹配其 Tittle 拼写）
                    tittle_div = a.select_one('.subTongzhiTextItemTittle')
                    title = tittle_div.get_text(strip=True) if tittle_div else a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    # 过滤末尾的分页导航干扰项
                    if len(title) < 6 or any(word in title for word in ['最前页', '上一页', '下一页', '最后页']):
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：.subTongzhiTextItemDate 已经是格式规范的时间
                    date_div = a.select_one('.subTongzhiTextItemDate')
                    date_text = date_div.get_text(strip=True) if date_div else self._extract_date_from_element(a)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 北交大卓越工程师院 .subTongzhiTextList 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：北交大交通运输学院 .sub_list + .list_down 结构 ====================
        bjtu_items = soup.select('ul.sub_list li.list_down, li.list_down')
        if bjtu_items:
            # 确认其是否具备特征子类：.list_down_title 与 .list_down_btn
            first_item = bjtu_items[0]
            if first_item.select_one('.list_down_title') and first_item.select_one('.list_down_btn'):
                current = []
                for li in bjtu_items:
                    a = li.select_one('.list_down_title a') or li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：.list_down_btn 中 a 内部被中括号包裹的日期 [YYYY-MM-DD]
                    date_btn = li.select_one('.list_down_btn a') or li.select_one('.list_down_btn')
                    date_text = ""
                    if date_btn:
                        date_text = date_btn.get_text(strip=True).strip('[]')
                    else:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 北交大交通运输院 .sub_list 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：内蒙古科技大学文法学院 li + a[href*="info/"] + span 结构 ====================
        ust_items = soup.select('ul li:has(span)')
        if not ust_items:
            # 兜底：寻找包含至少一个 span，且拥有至少两个 a 标签的 li
            ust_items = [li for li in soup.select('li') if li.select_one('span') and len(li.find_all('a')) >= 2]

        if ust_items:
            # 校验首个元素是否符合特征布局（包含 info 详情页链接以及含有数字年份的 span 容器）
            first_item = ust_items[0]
            info_a = first_item.find('a', href=lambda h: h and ('info/' in h or 'http' in h))
            span_tag = first_item.select_one('span')

            if info_a and span_tag and re.search(r'\d{4}', span_tag.get_text()):
                current = []
                for li in ust_items:
                    # 过滤查找真正的详情 a 链接（跳过类似 tzgg1.htm 的普通分类页）
                    valid_a = None
                    a_tags = li.find_all('a', href=True)
                    for a in a_tags:
                        href = a.get('href', '')
                        if 'info/' in href or 'http' in href:
                            valid_a = a
                            break

                    if not valid_a:
                        continue

                    # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                    title = valid_a.get('title', '').strip()
                    if not title:
                        title = valid_a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, valid_a.get('href', '').strip())

                    # 日期直接提取自行内唯一的 span
                    span_tag = li.select_one('span')
                    date_text = span_tag.get_text(strip=True) if span_tag else self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 内蒙古科大文法院 li+span 策略定位命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：内蒙古师大体育院 .mm + .dateR 复合结构 ====================
        pe_items = soup.select('.mm div[id^="line_u"], div.mm div[id^="line_u"]')
        if not pe_items:
            # 兜底：匹配任意列表中拥有类名 dateR 且包含有效链接的行
            pe_items = [div for div in soup.select('div') if div.select_one('.dateR') and div.find('a', href=True)]

        if pe_items:
            current = []
            for div in pe_items:
                a = div.find('a', href=True)
                if not a:
                    continue

                # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                title = a.get('title', '').strip()
                if not title:
                    title = a.get_text(strip=True)

                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', '').strip())

                # 日期提取并标准化：处理类似 "22/03/18 16:41:47" 中的 2 位数年份缩写
                date_text = ""
                date_div = div.select_one('.dateR')
                if date_div:
                    raw_date = date_div.get_text(strip=True)
                    # 智能拦截 YY/MM/DD 并标准化重组为 20YY-MM-DD
                    m_yy = re.match(r'^(\d{2})[-/.](\d{2})[-/.](\d{2})', raw_date)
                    if m_yy:
                        date_text = f"20{m_yy.group(1)}-{m_yy.group(2)}-{m_yy.group(3)}"
                    else:
                        date_text = raw_date
                else:
                    date_text = self._extract_date_from_element(div)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] 内蒙古师大体育院 .mm 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：内蒙古师范大学 a.wl + data-aos 复合结构 ====================
        wl_items = soup.select('li a.wl, a.wl')
        if wl_items:
            # 校验该卡片列表是否具有特征日期和标题结构
            first_item = wl_items[0]
            if first_item.select_one('.span.flex') or first_item.select_one('.txt1') or first_item.select_one(
                    'span.flex i'):
                current = []
                for a in wl_items:
                    # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                    title = a.get('title', '').strip()
                    if not title:
                        h3 = a.select_one('h3.txt1') or a.select_one('.txt1') or a.select_one('h3')
                        title = h3.get_text(strip=True) if h3 else a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：重组 span.flex 内的 i (如 20) 与其余文本 (如 2025-11)
                    date_text = ""
                    span_date = a.select_one('.span.flex span.flex') or a.select_one('span.flex')
                    if span_date:
                        i_tag = span_date.select_one('i')
                        full_span_text = span_date.get_text(strip=True)  # 如 "20/2025-11"
                        if i_tag:
                            day = i_tag.get_text(strip=True)
                            # 剔除天数和斜杠，只留下年月信息
                            year_month = full_span_text.replace(day, "").replace('/', '').strip()
                            if day and year_month:
                                # 补零合并为标准格式 YYYY-MM-DD
                                date_text = f"{year_month}-{day.zfill(2)}"

                    # 兜底提取
                    if not date_text:
                        date_text = self._extract_date_from_element(a)

                    current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] 内蒙古师范大学 a.wl 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：内工大人文院 li + float:right 复合结构 ====================
        float_right_items = soup.select('ul li:has(a[style*="float:right"]), li:has(a[style*="float:right"])')
        if not float_right_items:
            # 兜底：在无法使用 :has 伪类时的通用匹配，寻找含有右浮动 a 样式且包含有效 a 链接的 li 标签
            float_right_items = [li for li in soup.select('li') if
                                 li.select_one('a[style*="float:right"]') or li.select_one('a[style*="float: right"]')]

        if float_right_items:
            current = []
            for li in float_right_items:
                # 寻找行内的真实公告 a 标签（跳过 tzgg 分类链接和右浮动日期链接）
                valid_a = None
                a_tags = li.find_all('a', href=True)
                for a in a_tags:
                    href = a.get('href', '')
                    style = a.get('style', '') or ''
                    if 'tzgg.htm' not in href and 'float' not in str(style):
                        valid_a = a
                        break

                if not valid_a:
                    continue

                # 优先获取 a 标签完整的 title 属性，防字符截断
                title = valid_a.get('title', '').strip()
                if not title:
                    title = valid_a.get_text(strip=True)

                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, valid_a.get('href', '').strip())

                # 日期提取：提取带有右浮动 style 属性的 a 标签
                date_a = li.select_one('a[style*="float:right"], a[style*="float: right"]')
                date_text = date_a.get_text(strip=True) if date_a else self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] 内工大人文院 li + float:right 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：内蒙古大学研究生院 h2.jzf_d 复合结构 ====================
        jzf_items = soup.select('li:has(h2.jzf_d), li:has(.jzf_d)')
        if not jzf_items:
            # 兜底：匹配任意列表中拥有类名 jzf_d 且包含有效链接的行
            jzf_items = [li for li in soup.select('li') if li.select_one('h2.jzf_d') and li.find('a', href=True)]

        if jzf_items:
            current = []
            for li in jzf_items:
                a = li.find('a', href=True)
                if not a:
                    continue

                # 优先获取 a 标签完整的 title 属性，防字符截断
                title = a.get('title', '').strip()
                if not title:
                    h2 = a.select_one('h2.jzf_d') or a.select_one('.jzf_d')
                    title = h2.get_text(strip=True) if h2 else a.get_text(strip=True)

                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', '').strip())

                # 日期重组：.date 内部的 p 为年-月，span 为天
                date_text = ""
                date_div = a.select_one('.time .date') or a.select_one('.date')
                if date_div:
                    day_span = date_div.select_one('span')
                    ym_p = date_div.select_one('p')
                    if day_span and ym_p:
                        day = day_span.get_text(strip=True)
                        year_month = ym_p.get_text(strip=True).replace('/', '-')
                        if day and year_month:
                            # 补零并合并为标准格式 YYYY-MM-DD
                            date_text = f"{year_month}-{day.zfill(2)}"

                # 兜底提取
                if not date_text:
                    date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] 内蒙古大学研究生院 h2.jzf_d 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：通用 span.list_date 极简文字列表结构 ====================
        list_date_items = soup.select('li:has(span.list_date), li:has(.list_date)')
        if not list_date_items:
            # 兜底：匹配任意列表中拥有类名 list_date 且包含有效链接的行
            list_date_items = [li for li in soup.select('li') if
                               li.select_one('.list_date') and li.find('a', href=True)]

        if list_date_items:
            current = []
            for li in list_date_items:
                a = li.find('a', href=True)
                if not a:
                    continue

                # 优先获取 a 标签完整的 title 属性，防字符截断
                title = a.get('title', '').strip()
                if not title:
                    title = a.get_text(strip=True)

                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', '').strip())

                # 直接精准提取完整日期：span.list_date
                date_span = li.select_one('.list_date')
                date_text = date_span.get_text(strip=True) if date_span else self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] 极简文字列表 .list_date 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：内蒙古大学公共管理学院 a.flex + .time 复合结构 ====================
        flex_items = soup.select('ul li a.flex, li a.flex, a.flex')
        if flex_items:
            # 校验该卡片列表是否具有特征日期结构
            first_item = flex_items[0]
            if first_item.select_one('.time .day') and first_item.select_one('.time .year'):
                current = []
                for a in flex_items:
                    # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                    title = a.get('title', '').strip()
                    if not title:
                        p_tit = a.select_one('p.reveal1') or a.select_one('.reveal1')
                        title = p_tit.get_text(strip=True) if p_tit else a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：重组 .time 内的 .year (如 2025.02) 和 .day (如 20)
                    date_text = ""
                    time_div = a.select_one('.time')
                    if time_div:
                        day_div = time_div.select_one('.day')
                        year_div = time_div.select_one('.year')
                        if day_div and year_div:
                            day = day_div.get_text(strip=True)
                            year_month = year_div.get_text(strip=True).replace('.', '-')
                            if day and year_month:
                                # 补零合并为标准格式 YYYY-MM-DD
                                date_text = f"{year_month}-{day.zfill(2)}"

                    # 兜底提取
                    if not date_text:
                        date_text = self._extract_date_from_element(a)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 内大公管院 a.flex 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：内蒙古农大职院 .wel-news-box + a.item 复合结构 ====================
        vtc_items = soup.select('.wel-news-box a.item, .wel-news-box a')
        if vtc_items:
            # 校验该卡片列表是否具有特征日期列与标题列
            first_item = vtc_items[0]
            if first_item.select_one('.left .num') and first_item.select_one('.right .h0'):
                current = []
                for a in vtc_items:
                    # 定位获取卡片右侧的 h0 节点作为标题
                    h0 = a.select_one('.right .h0') or a.select_one('.h0')
                    title = h0.get_text(strip=True) if h0 else a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：重组左侧 .time (如 2025-09) 和 .num (如 03)
                    date_text = ""
                    num_div = a.select_one('.left .num') or a.select_one('.num')
                    time_div = a.select_one('.left .time') or a.select_one('.time')
                    if num_div and time_div:
                        day = num_div.get_text(strip=True)
                        year_month = time_div.get_text(strip=True).replace('/', '-')
                        if day and year_month:
                            # 补零合并为标准格式 YYYY-MM-DD
                            date_text = f"{year_month}-{day.zfill(2)}"

                    # 兜底提取
                    if not date_text:
                        date_text = self._extract_date_from_element(a)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 内蒙古农大职院 .wel-news-box 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：内蒙古农业大学 a 嵌套 li + table.time 复合结构 ====================
        imau_items = soup.select('ul.data-list a[href], .data-list a[href]')
        if imau_items:
            # 校验该非标列表是否拥有 time 表格中独立的 day 与 year 单元格
            first_item = imau_items[0]
            if first_item.select_one('span.time td.day') and first_item.select_one('span.time td.year'):
                current = []
                for a in imau_items:
                    # 强力技巧：优先提取 a 标签上未被前端省略号截断的 title 完整属性值
                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 3 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：提取 td.year (如 2026/03) 转换为连字符格式，并拼装 td.day (如 06)
                    date_text = ""
                    day_td = a.select_one('td.day')
                    year_td = a.select_one('td.year')
                    if day_td and year_td:
                        day = day_td.get_text(strip=True)
                        year_month = year_td.get_text(strip=True).replace('/', '-')
                        if day and year_month:
                            # 规范化拼合，如 "2026-03" + "06" -> "2026-03-06"
                            date_text = f"{year_month}-{day.zfill(2)}"

                    # 兜底提取
                    if not date_text:
                        date_text = self._extract_date_from_element(a)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 内蒙古农大 .data-list 表格策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：兰州财经大学 .content-r-con-li 复合结构 ====================
        r_con_items = soup.select('.list-content-r-con li.content-r-con-li, li.content-r-con-li')
        if r_con_items:
            # 校验该列表行是否具备 .con-li-nr 核心节点样式
            first_item = r_con_items[0]
            if first_item.select_one('.con-li-nr') and first_item.find('a', href=True):
                current = []
                for li in r_con_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签完整的 title 属性，防止文本溢出截断
                    title = a.get('title', '').strip()
                    if not title:
                        tit_p = a.select_one('.tit p') or a.select_one('.tit')
                        title = tit_p.get_text(strip=True) if tit_p else a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：.con-li-nr .time 中的中文日期
                    date_div = li.select_one('.con-li-nr .time') or li.select_one('.time')
                    date_text = date_div.get_text(strip=True) if date_div else self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 兰州财大 .content-r-con-li 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：兰州理工生命院 .dete + .text 复合结构 ====================
        dete_items = soup.select('li:has(.dete):has(.text)')
        if not dete_items:
            # 兜底：寻找同时包含日期类 .dete 与正文类 .text 节点的 li 标签
            dete_items = [li for li in soup.select('li') if li.select_one('.dete') and li.select_one('.text')]

        if dete_items:
            current = []
            for li in dete_items:
                a = li.find('a', href=True)
                if not a:
                    continue

                # 提取标题：精准锁定在 .text 容器内部的 .title 中，防 .desc 预览杂质干扰
                title_div = a.select_one('.text .title') or a.select_one('.title')
                title = title_div.get_text(strip=True) if title_div else a.get_text(strip=True)

                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', '').strip())

                # 日期重组：.dete 中的 .y (年-月) 与 .d (日) 拼接
                date_text = ""
                dete_div = a.select_one('.dete')
                if dete_div:
                    d_elem = dete_div.select_one('.d')
                    y_elem = dete_div.select_one('.y')
                    if d_elem and y_elem:
                        day = d_elem.get_text(strip=True)
                        year_month = y_elem.get_text(strip=True).replace('/', '-')
                        if day and year_month:
                            # 规范化补零并拼合
                            date_text = f"{year_month}-{day.zfill(2)}"

                # 兜底提取
                if not date_text:
                    date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] 兰州理工生命院 .dete 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：div.box + line_u8 + div.time ====================
        box_items = soup.select('div.box[id^="line_u8"], div.box')
        if box_items:
            current = []
            for box in box_items:
                a = box.find('a', href=True)
                if not a:
                    continue

                # 标题
                title_div = box.select_one('.title')
                title = title_div.get_text(strip=True) if title_div else a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期
                date_text = self._extract_date_from_element(box)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 2:
                self.log.emit(f"[成功] div.box + line_u8 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：云财大法政学院 .tm + .text 复合结构 ====================
        tm_items = soup.select('li:has(.tm):has(.text)')
        if not tm_items:
            # 兜底：在无法使用 :has 伪类时的通用匹配，寻找同时含有 .tm 和 .text 节点的 li 标签
            tm_items = [li for li in soup.select('li') if li.select_one('.tm') and li.select_one('.text')]

        if tm_items:
            current = []
            for li in tm_items:
                a = li.find('a', href=True)
                if not a:
                    continue

                # 优先获取 100% 完整的 title 属性，防止文字被前端截断
                title = a.get('title', '').strip()
                if not title:
                    p_text = a.select_one('.text p') or a.select_one('.text')
                    title = p_text.get_text(strip=True) if p_text else a.get_text(strip=True)

                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', '').strip())

                # 日期重组：.tm 内部的 span.big 为年-月，另一个无 big 类的 span 为天
                date_text = ""
                tm_div = a.select_one('.tm')
                if tm_div:
                    big_span = tm_div.select_one('span.big')
                    # 寻找不含 big 样式的另一个 span
                    spans = tm_div.find_all('span')
                    day_span = None
                    for s in spans:
                        # 兼容 class 为空或不含 big 的情况
                        s_classes = s.get('class', [])
                        if not s_classes or 'big' not in s_classes:
                            day_span = s
                            break

                    if big_span and day_span:
                        day = day_span.get_text(strip=True)
                        year_month = big_span.get_text(strip=True).replace('/', '-')
                        if day and year_month:
                            # 规范化拼合，如 "2026-04" + "09" -> "2026-04-09"
                            date_text = f"{year_month}-{day.zfill(2)}"

                # 兜底提取
                if not date_text:
                    date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] 云财大法政院 .tm 复合列表策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：云财大国商院 .gdlr-core-event-item-list-gg 复合结构 ====================
        gdlr_items = soup.select('.gdlr-core-event-item-list-gg, div.gdlr-core-event-item-list-gg')
        if not gdlr_items:
            # 兜底：匹配任意包含 gdlr-core 样式的事件节点
            gdlr_items = soup.select('.gdlr-core-event-item-list')

        if gdlr_items:
            current = []
            for item in gdlr_items:
                a = item.find('a', href=True)
                if not a:
                    continue

                # 优先获取 a 标签完整的 title 属性，防字符溢出截断
                title = a.get('title', '').strip()
                if not title:
                    h3 = a.select_one('h3')
                    title = h3.get_text(strip=True) if h3 else a.get_text(strip=True)

                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', '').strip())

                # 日期重组：提取 .gdlr-core-month (如 2026/05) 并将 '/' 替换为 '-' 拼装上 .gdlr-core-date (如 07)
                date_text = ""
                date_span = item.select_one('.gdlr-core-date')
                month_span = item.select_one('.gdlr-core-month')
                if date_span and month_span:
                    day = date_span.get_text(strip=True)
                    year_month = month_span.get_text(strip=True).replace('/', '-')
                    if day and year_month:
                        # 补零合并为标准 YYYY-MM-DD 格式
                        date_text = f"{year_month}-{day.zfill(2)}"

                # 兜底提取
                if not date_text:
                    date_text = self._extract_date_from_element(item)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] 云财大国商院 .gdlr-core-event 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：VSB 通用 winstyle 表格结构（物电学院等） ====================
        # 使用类名前缀选择器，精准兼容不同后缀数字的 winstyle 列表样式
        win_items = soup.select('table[class^="winstyle"] tr, table.winstyle140620 tr')
        if win_items:
            # 校验行内是否具备特有的 timestyle 日期类或链接类
            first_item = None
            for item in win_items:
                if item.select_one('span[class^="timestyle"]') or item.select_one('a[class^="c"]'):
                    first_item = item
                    break

            if first_item:
                current = []
                for tr in win_items:
                    a = tr.find('a', href=True)
                    date_span = tr.select_one('span[class^="timestyle"]')
                    if not a or not date_span:
                        continue

                    # 优先获取干净的 title 属性，防止文本中含有过多后置空格或换行干扰
                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取并清理 HTML 空格实体
                    date_text = date_span.get_text(strip=True).replace('\xa0', '').strip()

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] VSB winstyle 系统表格策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：云师大文学院 li.btlba 列表结构 ====================
        btlba_items = soup.select('li.btlba')
        if btlba_items:
            # 校验该列表行是否具备 Layui-icon 特征列和日期列
            first_item = btlba_items[0]
            if first_item.select_one('.btlbb') and first_item.select_one('.btlbccc'):
                current = []
                for li in btlba_items:
                    a = li.select_one('.btlbb a') or li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 title 属性，若无则使用节点文本
                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期直接提取自右对齐的 dl.btlbccc 节点
                    date_dl = li.select_one('.btlbccc')
                    date_text = date_dl.get_text(strip=True) if date_dl else self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 云师大文学院 li.btlba 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：云师大信息学院 li.rlb 复合结构 ====================
        rlb_items = soup.select('li.rlb')
        if rlb_items:
            # 校验该列表行是否具备独立的 sjxs 日期结构和标题 a 标签
            first_item = rlb_items[0]
            if first_item.select_one('.sjxs') and first_item.select_one('h3 a'):
                current = []
                for li in rlb_items:
                    a = li.select_one('h3 a') or li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签的 title 属性作为标题，防止文本被前端截断
                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：.sjxs 内部的 dt 包含月.日（需要转换为 -），dd 包含年
                    date_text = ""
                    sjxs = li.select_one('.sjxs')
                    if sjxs:
                        dt = sjxs.select_one('dt')
                        dd = sjxs.select_one('dd')
                        if dt and dd:
                            month_day = dt.get_text(strip=True).replace('.', '-')  # "05-13"
                            year = dd.get_text(strip=True)  # "2026"
                            if month_day and year:
                                date_text = f"{year}-{month_day}"

                    # 兜底提取
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 云师大信息院 li.rlb 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：云大材料与能源学院 aos动画列表 结构 ====================
        aos_items = soup.select('li[data-aos="fade-up"], li[data-aos]')
        if aos_items:
            # 校验该列表行是否具备 .eclip 类和独立的 time 标签
            first_item = aos_items[0]
            if first_item.select_one('a.eclip') and first_item.select_one('time'):
                current = []
                for li in aos_items:
                    a = li.select_one('a.eclip') or li.find('a', href=True)
                    if not a:
                        continue

                    title = a.get_text(strip=True)
                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期直接提取自 time 标签，格式本身即标准
                    time_tag = li.select_one('time')
                    date_text = time_tag.get_text(strip=True) if time_tag else self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 云大材能院 li[data-aos] 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：云大新闻学院 .panel-body 列表结构 ====================
        panel_items = soup.select('.panel-body li[id^="line_u"], .panel-body li')
        if panel_items:
            # 校验该列表行是否具备独立的 span.time
            first_item = panel_items[0]
            if first_item.select_one('span.time') and first_item.find('a', href=True):
                current = []
                for li in panel_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 优先获取 a 标签的 title 属性作为标题，防止文本过长时被前端截断
                    title = a.get('title', '').strip()
                    if not title:
                        title = a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期提取：span.time 中的 [YYYY-MM-DD]，使用 .strip('[]') 去除括号
                    date_span = li.select_one('span.time, .time')
                    date_text = ""
                    if date_span:
                        date_text = date_span.get_text(strip=True).strip('[]')
                    else:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 3:
                    self.log.emit(f"[成功] 云大新闻院 .panel-body 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：云大古生物研究院 .inner-list1 + li 结构 ====================
        inner_items = soup.select('.inner-list1 li[id^="line_u"]')
        if not inner_items:
            # 兜底：匹配任意列表中，日期使用 div.date 下的 span 和 i 表示的结构
            inner_items = soup.select('li:has(.date span):has(.date i)')

        if inner_items:
            current = []
            for li in inner_items:
                a = li.find('a', href=True)
                if not a:
                    continue

                # 提取标题（位于 .text h6 内部）
                h6 = a.select_one('.text h6') or a.select_one('h6')
                title = h6.get_text(strip=True) if h6 else a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', '').strip())

                # 日期重组：div.date 中的 span 为日，i 为年-月
                date_text = ""
                date_div = a.select_one('.date')
                if date_div:
                    day_elem = date_div.select_one('span')
                    ym_elem = date_div.select_one('i')
                    if day_elem and ym_elem:
                        day = day_elem.get_text(strip=True)
                        year_month = ym_elem.get_text(strip=True).replace('/', '-')
                        if day and year_month:
                            # 补零并合并为标准 YYYY-MM-DD
                            date_text = f"{year_month}-{day.zfill(2)}"

                # 兜底提取
                if not date_text:
                    date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 1:
                self.log.emit(f"[成功] 云大古生物院 .inner-list1 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：云大历史与档案学院 table-striped 表格结构 ====================
        # 精准匹配包含样式 .table-striped 的表格或 line_u3 所在的行
        striped_rows = soup.select('table.table-striped tr[id^="line_u"], tr[id^="line_u3_"]')
        if not striped_rows:
            # 兜底：匹配任意表格中带有 line_u 标识的行
            striped_rows = soup.select('table tr[id^="line_u"]')

        if striped_rows:
            current = []
            for tr in striped_rows:
                tds = tr.find_all('td')
                if len(tds) < 2:
                    continue

                # 寻找行内的 a 标签（通常在第一个单元格）
                a = tr.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                # 过滤掉一些辅助类文字
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', '').strip())

                # 日期在最后一个单元格中
                date_text = tds[-1].get_text(strip=True)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] 云大历史学院 .table-striped 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：云南大学农学院 .date2 + a.text 复合结构 ====================
        ynu_items = soup.select('ul li:has(.date2), li:has(.date2)')
        if ynu_items:
            # 校验该列表行是否具备 .date2 和标题容器
            first_item = ynu_items[0]
            if first_item.select_one('.date2') and (first_item.select_one('.title') or first_item.select_one('a.text')):
                current = []
                for li in ynu_items:
                    a = li.select_one('a.text') or li.find('a', href=True)
                    if not a:
                        continue

                    # 提取标题
                    title_elem = a.select_one('.title') or a
                    title = title_elem.get_text(strip=True) if title_elem else a.get_text(strip=True)
                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期重组：.date2 内首个 p 标签为日，尾个 p 标签为年-月
                    date_text = ""
                    date2_div = li.select_one('.date2')
                    if date2_div:
                        p_tags = date2_div.find_all('p')
                        if len(p_tags) >= 2:
                            day = p_tags[0].get_text(strip=True)
                            year_month = p_tags[-1].get_text(strip=True).replace('/', '-')
                            if day and year_month:
                                # 补零后拼合
                                date_text = f"{year_month}-{day.zfill(2)}"

                    # 兜底提取
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 1:
                    self.log.emit(f"[成功] 云南大学农学院 .date2 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：中山大学 div.list-page + posttime ====================
        page_items = soup.select('div.list-page')
        if page_items:
            current = []
            for item in page_items:
                a = item.select_one('p.title a')
                if not a:
                    a = item.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期提取
                date_text = self._extract_date_from_element(item)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 2:
                self.log.emit(f"[成功] div.list-page + posttime 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：中山大学中法核学院 .termlist 复合结构 ====================
        term_items = soup.select('.termlist')
        if term_items:
            # 校验是否为带 daytime 和 title 的目标行
            first_item = term_items[0]
            if first_item.select_one('.daytime') and first_item.select_one('.title'):
                current = []
                for item in term_items:
                    a = item.select_one('.title a') or item.find('a', href=True)
                    if not a:
                        continue

                    title = a.get_text(strip=True)
                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', '').strip())

                    # 日期重组：.monthyear (如 2025-12) + .day (如 09)
                    date_text = ""
                    day_elem = item.select_one('.day')
                    my_elem = item.select_one('.monthyear')
                    if day_elem and my_elem:
                        day = day_elem.get_text(strip=True)
                        year_month = my_elem.get_text(strip=True).replace('/', '-')
                        if day and year_month:
                            # 补零并合并
                            date_text = f"{year_month}-{day.zfill(2)}"

                    # 兜底日期提取
                    if not date_text:
                        date_text = self._extract_date_from_element(item)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 1:
                    self.log.emit(f"[成功] 中山大学中法核学院 .termlist 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：中山医学院 list-page.termlist ====================
        term_items = soup.select('div.list-page.termlist')
        if term_items:
            current = []
            for item in term_items:
                a = item.select_one('p.title a')
                if not a:
                    a = item.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期提取
                date_text = self._extract_date_from_element(item)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 2:
                self.log.emit(f"[成功] div.list-page.termlist 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：中山大学 Drupal Views .views-row 结构 ====================
        views_items = soup.select('.view-content .views-row, .view-rows .views-row')
        if views_items:
            # 校验是否具有该结构的特征样式
            first_item = views_items[0]
            if first_item.select_one('.main-left-title') or first_item.select_one('.main-left-time'):
                current = []
                for row in views_items:
                    a = row.select_one('.main-left-title a') or row.find('a', href=True)
                    if not a:
                        continue

                    title = a.get_text(strip=True)
                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    # 清理链接可能存在的首尾空格
                    url = urljoin(base_url, a.get('href', '').strip())

                    # 直接精准提取完整日期：.main-left-time
                    date_span = row.select_one('.main-left-time')
                    date_text = date_span.get_text(strip=True) if date_span else self._extract_date_from_element(row)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 1:
                    self.log.emit(f"[成功] 中山大学 Drupal Views 策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：中山大学 ul.list-content + span.date ====================
        list_items = soup.select('ul.list-content li.list-item, li.list-item')
        if list_items:
            current = []
            for li in list_items:
                a = li.find('a', href=True)
                if not a:
                    continue

                # 标题（从 p 标签提取更干净）
                p_title = a.select_one('p')
                title = p_title.get_text(strip=True) if p_title else a.get_text(strip=True)

                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期
                date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 1:
                self.log.emit(f"[成功] ul.list-content + span.date 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：line_uX + .con .tit + p.time 复合解析结构 ====================
        line_u_items = soup.select('li[id^="line_u"]')
        if line_u_items:
            # 只有当包含复合结构时才启用此策略，避免干扰普通的 line_u 列表
            first_item = line_u_items[0]
            if first_item.select_one('.con .tit') or first_item.select_one('.tit') or first_item.select_one('p.time'):
                current = []
                for li in line_u_items:
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 优先利用 a 标签的 title 属性直接获取干净完整的标题
                    title = a.get('title', '').strip()
                    if not title:
                        title_elem = a.select_one('.tit') or a.select_one('.con .tit')
                        title = title_elem.get_text(strip=True) if title_elem else a.get_text(strip=True)

                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回', '查看更多']:
                        continue

                    url = urljoin(base_url, a.get('href', ''))

                    # 智能提取并重组日期：<p class="time"><span>04-02</span>2024</p>
                    date_text = ""
                    time_p = a.select_one('p.time, .time')
                    if time_p:
                        span = time_p.select_one('span')
                        p_text = time_p.get_text(strip=True)
                        if span:
                            md = span.get_text(strip=True)  # "04-02"
                            year = p_text.replace(md, "").strip()  # 剔除月日后剩下 "2024"
                            # 校验提取的数据合法性
                            if year.isdigit() and re.match(r'\d{1,2}-\d{1,2}', md):
                                date_text = f"{year}-{md}"

                    # 兜底提取
                    if not date_text:
                        date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

                if len(current) >= 1:
                    self.log.emit(f"[成功] line_u + .tit 复合竞赛样式策略命中 → {len(current)} 条")
                    return current

        # ==================== 1. 新增：中央美术学院 bricks 卡片结构 ====================
        brick_articles = soup.select('article.brick.entry, section#bricks article')
        if brick_articles:
            current = []
            for article in brick_articles:
                a = article.select_one('h1.entry-title a, .entry-title a')
                if not a:
                    a = article.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期
                date_text = self._extract_date_from_element(article)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 2:
                self.log.emit(f"[成功] 中央美术学院 bricks 卡片策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：div.jzlb + .btt3 + .fbsj4 ====================
        jz_items = soup.select('div.jzlb')
        if jz_items:
            current = []
            for div in jz_items:
                # 标题
                a = div.select_one('div.btt3 a')
                if not a:
                    a = div.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期
                date_text = self._extract_date_from_element(div)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 2:
                self.log.emit(f"[成功] div.jzlb + .fbsj4 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：ul.b-list ====================
        b_list_items = soup.select('ul.b-list li')
        if b_list_items:
            current = []
            for li in b_list_items:
                a = li.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期提取（span.m-date 常为空，从标题提取年份）
                date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 2:
                self.log.emit(f"[成功] ul.b-list 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：国科大杭高院 .list_notice + a 结构 ====================
        hn_items = soup.select('.list_notice a')
        if hn_items:
            current = []
            for a in hn_items:
                # 提取标题
                h3 = a.select_one('.txt h3') or a.select_one('h3')
                title = h3.get_text(strip=True) if h3 else a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 智能提取并组合拆分型日期：<time><em>24</em><p>2025-10</p></time>
                date_text = ""
                time_tag = a.select_one('time')
                if time_tag:
                    em = time_tag.select_one('em')
                    p = time_tag.select_one('p')
                    if em and p:
                        day = em.get_text(strip=True)
                        year_month = p.get_text(strip=True).replace('/', '-')
                        if day and year_month:
                            # 补零并拼接，如 "2025-10" + "24" -> "2025-10-24"
                            date_text = f"{year_month}-{day.zfill(2)}"

                # 兜底日期提取
                if not date_text:
                    date_text = self._extract_date_from_element(a)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] 国科大杭高院 .list_notice 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：国科大南京学院 .list_time + a.item 结构 ====================
        njc_items = soup.select('.list_time a.item, .list_time a')
        if njc_items:
            current = []
            for a in njc_items:
                # 提取标题
                h3 = a.select_one('.txt h3') or a.select_one('h3')
                title = h3.get_text(strip=True) if h3 else a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 智能组合拆分型日期：<time><h5><span>06</span>03</h5><p>2025</p></time>
                date_text = ""
                time_tag = a.select_one('time')
                if time_tag:
                    h5 = time_tag.select_one('h5')
                    p = time_tag.select_one('p')
                    if h5 and p:
                        span = h5.select_one('span')
                        year = p.get_text(strip=True)
                        if span:
                            month = span.get_text(strip=True)
                            h5_full = h5.get_text(strip=True)
                            # 从整个 h5 文本中去掉月份，剩下的就是天数（例如从"0603"中删掉"06"得到"03"）
                            day = h5_full.replace(month, "").strip()
                            # 校验提取出来的是否均为数字，以防提取到杂质
                            if year.isdigit() and month.isdigit() and day.isdigit():
                                date_text = f"{year}-{month.zfill(2)}-{day.zfill(2)}"

                # 如果特殊拆分组合失败，使用原有通用日期函数进行提取兜底
                if not date_text:
                    date_text = self._extract_date_from_element(a)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] 国科大南京学院 .list_time 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 纳米能源所专用：.hui12_sj2 表格或含有 .huizw11_sj 行的行内解析 ====================
        # 优先使用 table 样式名定位，若无则使用日期样式反向找行
        hui_rows = soup.select('table.hui12_sj2 tr')
        if not hui_rows:
            huiz_tds = soup.select('.huizw11_sj, td.huizw11_sj')
            if huiz_tds:
                hui_rows = [td.parent for td in huiz_tds if td.parent]

        if hui_rows:
            current = []
            for tr in hui_rows:
                # 寻找行内的第一个有效链接（不再局限于 tds[0]）
                a = tr.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 直接定位该行中的日期单元格
                date_td = tr.select_one('.huizw11_sj, td.huizw11_sj')
                if date_td:
                    date_text = date_td.get_text(strip=True)
                else:
                    date_text = self._extract_date_from_element(tr)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .hui12_sj2 / .huizw11_sj 纳米能源所策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：div.right-list + .line-box-new ====================
        box_items = soup.select('div.line-box-new')
        if box_items:
            current = []
            for box in box_items:
                # 标题
                txt_span = box.select_one('span.txt')
                title = txt_span.get_text(strip=True) if txt_span else box.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                # 链接（onclick 或 a）
                url = ""
                onclick = box.get('onclick', '')
                if 'window.open' in onclick:
                    match = re.search(r"window\.open\(['\"](.*?)['\"]", onclick)
                    if match:
                        url = urljoin(base_url, match.group(1))
                else:
                    a = box.find('a', href=True)
                    if a:
                        url = urljoin(base_url, a.get('href', ''))

                if not url:
                    continue

                # 日期
                date_text = self._extract_date_from_element(box)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 2:
                self.log.emit(f"[成功] div.right-list + .line-box-new 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：blockquote.hero-unify + h4 a + span日期 ====================
        hero_items = soup.select('blockquote.hero-unify')
        if hero_items:
            current = []
            for block in hero_items:
                a = block.select_one('h4 a')
                if not a:
                    a = block.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期提取
                date_text = self._extract_date_from_element(block)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 2:
                self.log.emit(f"[成功] blockquote.hero-unify 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：ul.wp_article_list + .list_item + .Article_PublishDate ====================
        wp_items = soup.select('ul.wp_article_list li.list_item')
        if wp_items:
            current = []
            for li in wp_items:
                # 标题
                title_span = li.select_one('span.Article_Title a')
                title = title_span.get_text(strip=True) if title_span else ""
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                a = title_span or li.find('a', href=True)
                url = urljoin(base_url, a.get('href', '') if a else '')

                # 日期
                date_span = li.select_one('span.Article_PublishDate')
                date_text = date_span.get_text(strip=True) if date_span else self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] ul.wp_article_list 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：a.list_item + div.date + div.title ====================
        list_items = soup.select('a.list_item')
        if list_items:
            current = []
            for a in list_items:
                # 标题
                title_div = a.select_one('.title')
                title = title_div.get_text(strip=True) if title_div else a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期
                date_text = self._extract_date_from_element(a)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 2:
                self.log.emit(f"[成功] a.list_item + div.date 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：Bootstrap卡片结构 (col > card > h5.card-title) ====================
        card_items = soup.select('.card, div.col-xl-3, div.col-md-6')
        if card_items:
            current = []
            for card in card_items:
                a = card.select_one('h5.card-title a') or card.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期提取
                date_text = self._extract_date_from_element(card)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 2:
                self.log.emit(f"[成功] Bootstrap Card 卡片结构策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：div.newsmain + a.newsinfo ====================
        newsinfo_items = soup.select('div.newsmain a.newsinfo')
        if newsinfo_items:
            current = []
            for a in newsinfo_items:
                # 标题
                names = a.select_one('.names')
                title = names.get_text(strip=True) if names else a.get('title', '') or a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期提取
                date_text = self._extract_date_from_element(a)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 2:
                self.log.emit(f"[成功] div.newsmain + a.newsinfo 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：div.list_time.f0 + time > em + p ====================
        time_items = soup.select('div.list_time.f0 a, div.list_time a')
        if time_items:
            current = []
            for a in time_items:
                # 标题
                h3 = a.select_one('h3')
                title = h3.get_text(strip=True) if h3 else a.get('title', '') or a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期提取
                date_text = self._extract_date_from_element(a.parent)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 2:
                self.log.emit(f"[成功] div.list_time.f0 + time 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：嵌套 table + tr + td + div[white-space:nowrap] ====================
        table_rows = soup.select('table tr')
        if table_rows:
            current = []
            for tr in table_rows:
                tds = tr.find_all('td')
                if len(tds) < 2:
                    continue

                # 标题在第一个 td 的 a
                a = tds[0].find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期在第二个 td 的 div
                date_text = ""
                if len(tds) >= 2:
                    div_date = tds[1].select_one('div[style*="white-space:nowrap"]')
                    if div_date:
                        date_text = div_date.get_text(strip=True)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] 嵌套 table + tr + td 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：ul.cp_list clearfix ====================
        cp_items = soup.select('ul.cp_list li')
        if cp_items:
            current = []
            for li in cp_items:
                a = li.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期提取（标题中常包含年份）
                date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 2:
                self.log.emit(f"[成功] ul.cp_list 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：ul.news_list.list2 + .news_meta（.news_year + .news_day） ====================
        news_items = soup.select('ul.news_list.list2 li.news, li.news.clearfix')
        if news_items:
            current = []
            for li in news_items:
                a = li.select_one('.news_title a') or li.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回', '查看更多']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期提取
                date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] ul.news_list.list2 + .news_meta 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：含 table 的 ul + li#line_uX + span[日期] ====================
        table_li_items = soup.select('ul li[id^="line_u"], table li[id^="line_u"], ul li')
        if table_li_items:
            current = []
            for li in table_li_items:
                a = li.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期提取
                date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 2:
                self.log.emit(f"[成功] 含table的 ul li#line_uX 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：li.wow.fadeInUp + span.right 日期 ====================
        wow_items = soup.select('li.wow.fadeInUp, ul li[class*="wow"]')
        if wow_items:
            current = []
            for li in wow_items:
                a = li.select_one('a.overhide') or li.find('a', href=True)
                if not a:
                    continue

                # 日期：span.right
                date_span = a.select_one('span.right')
                date_text = date_span.get_text(strip=True) if date_span else ""

                # 标题：去掉日期后的文本
                title = a.get_text(strip=True)
                if date_text:
                    title = title.replace(date_text, "").strip()

                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 2:
                self.log.emit(f"[成功] li.wow.fadeInUp + span.right 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：ul.main.portalList + li#line_u2 ====================
        portal_items = soup.select('ul.main.portalList li[id^="line_u"], ul.main.portalList li')
        if portal_items:
            current = []
            for li in portal_items:
                a = li.select_one('a[href]')
                if not a:
                    continue

                # 标题：h5
                h5 = li.select_one('h5')
                title = h5.get_text(strip=True) if h5 else a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期
                date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 2:
                self.log.emit(f"[成功] ul.main.portalList 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：ul.list + li#line_u7 + a > span 日期 ====================
        list_items = soup.select('ul.list li[id^="line_u"], ul.list li')
        if list_items:
            current = []
            for li in list_items:
                a = li.find('a', href=True)
                if not a:
                    continue

                # 日期：a 标签内的第一个 span
                date_span = a.select_one('span')
                date_text = date_span.get_text(strip=True) if date_span else ""

                # 标题：a 标签去掉日期后的文本
                title = a.get_text(strip=True)
                if date_text:
                    title = title.replace(date_text, "").strip()

                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 2:
                self.log.emit(f"[成功] ul.list + li#line_u7 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：announcements-list ====================
        ann_items = soup.select('ul.announcements-list li.announcement')
        if ann_items:
            current = []
            for li in ann_items:
                a = li.select_one('a.announcement-title')
                if not a:
                    a = li.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回', '查看详情']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期
                date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 2:
                self.log.emit(f"[成功] ul.announcements-list 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：ul.ss + li#line_u9 + span[width:19%] ====================
        ss_items = soup.select('ul.ss li[id^="line_u"], ul.ss li')
        if ss_items:
            current = []
            for li in ss_items:
                a = li.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or any(word in title for word in ['首页', '更多', '返回', '上一页', '下一页']):
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期提取
                date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 2:
                self.log.emit(f"[成功] ul.ss + li#line_u9 策略命中 → {len(current)} 条")
                return current

        # ==================== 新增：ul li > p.time + p.con 结构 ====================
        list_items = soup.select('ul li')
        if list_items:
            current = []
            for li in list_items:
                a = li.find('a', href=True)
                if not a:
                    continue

                # 标题在 .tit
                title_elem = li.select_one('.tit')
                title = title_elem.get_text(strip=True) if title_elem else a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期在 p.time
                time_p = li.select_one('p.time')
                date_text = ""
                if time_p:
                    span = time_p.select_one('span')
                    year_text = time_p.get_text(strip=True)
                    if span and year_text:
                        md = span.get_text(strip=True)  # 05-12
                        year = year_text.replace(md, "").strip()  # 2026
                        date_text = f"{year}-{md}"

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] ul li > p.time + p.con 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：mcon_titleicon2 + span.f_l / f_r ====================
        mcon_items = soup.select('.mcon_titleicon2 p[id^="line_u"]')
        if mcon_items:
            current = []
            for p in mcon_items:
                # 标题
                a = p.select_one('span.f_l a')
                if not a:
                    a = p.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期（span.f_r）
                date_text = self._extract_date_from_element(p)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .mcon_titleicon2 + span.f_r 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：ul > li > p > a + span 日期 ====================
        simple_li_items = soup.select('ul li')
        if simple_li_items:
            current = []
            for li in simple_li_items:
                # 标题：p 里面的 a
                a = li.select_one('p a') or li.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回', '最前', '上一页']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期：li 里的 span
                date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] ul > li > p > a + span 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：ul.ul-relative ====================
        ul_items = soup.select('ul.ul-relative li')
        if ul_items:
            current = []
            for li in ul_items:
                a = li.select_one('a[href]')
                if not a:
                    continue

                # 标题 - p 标签
                p_title = a.select_one('p')
                title = p_title.get_text(strip=True) if p_title else a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期 - span（颜色特定）
                date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] ul.ul-relative 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：m-list4 ====================
        mlist_items = soup.select('ul.m-list4 li')
        if mlist_items:
            current = []
            for li in mlist_items:
                a = li.select_one('a.con')
                if not a:
                    continue

                # 标题 - h3
                h3 = li.select_one('h3')
                title = h3.get_text(strip=True) if h3 else a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期
                date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] ul.m-list4 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：#list_container + .list_cont_li ====================
        list_items = soup.select('#list_container li.list_cont_li, ul#list_container li.list_cont_li')
        if list_items:
            current = []
            for li in list_items:
                # 标题
                a_title = li.select_one('a.p1')
                if not a_title:
                    a_title = li.find('a', class_=lambda x: x and 'p1' in str(x), href=True)
                if not a_title:
                    continue

                title = a_title.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a_title.get('href', ''))

                # 日期（<p>标签）
                date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] #list_container .list_cont_li 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增/强化：.article-list.dh + .txt-elise + .spans ====================
        article_items = soup.select('.article-list.dh li, .article-list li[id^="line_u"], ul.article-list li')
        if article_items:
            current = []
            for li in article_items:
                a = li.select_one('a.txt-elise') or li.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 提取日期（.spans）
                date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .article-list.dh + .txt-elise 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：.ny-news01 + .r01 + .l01（.n-day + .n-month） ====================
        ny_items = soup.select('ul.ny-news01 li')
        if ny_items:
            current = []
            for li in ny_items:
                a = li.select_one('.r01 a') or li.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 提取日期
                date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .ny-news01 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：.article-list + .txt-elise（无需日期） ====================
        article_items = soup.select('.article-list li, ul.article-list li, .list_news li')
        if article_items:
            current = []
            for li in article_items:
                a = li.select_one('a.txt-elise') or li.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 无日期
                date_text = ""

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .article-list + .txt-elise 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：.list_news（h2 a + span日期） ====================
        list_news_items = soup.select('ul.list_news li')
        if list_news_items:
            current = []
            for li in list_news_items:
                a = li.select_one('h2 a') or li.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期在 span
                date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .list_news 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：.newsIns + .nDate（<strong>日</strong><i>年.月</i>） ====================
        news_ins = soup.select('.newsIns')
        if news_ins:
            current = []
            for ins in news_ins:
                a = ins.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 提取日期
                date_text = self._extract_date_from_element(ins)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .newsIns + .nDate 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：datastore + record（CDATA动态列表） ====================
        xml_scripts = soup.select('script[type="text/xml"]')
        if xml_scripts:
            current = []
            for script in xml_scripts:
                text = script.get_text()
                # 提取所有 <record> 中的内容
                records = re.findall(r'<record><!\[CDATA\[(.*?)\]\]></record>', text, re.DOTALL | re.IGNORECASE)
                for record_html in records:
                    record_soup = BeautifulSoup(record_html, 'html.parser')
                    a = record_soup.find('a', href=True)
                    if not a:
                        continue

                    title = a.get_text(strip=True)
                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', ''))
                    date_text = self._extract_date_from_element(record_soup)  # 或从 span 提取

                    current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] datastore/record CDATA 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：.column-news-item（含 .column-news-title + .column-news-date） ====================
        column_items = soup.select('.column-news-item')
        if column_items:
            current = []
            for item in column_items:
                a = item.select_one('.column-news-title a') or item.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 提取日期
                date_text = self._extract_date_from_element(item)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .column-news-item 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增/强化：.news_list.list2.gsapdom（含 .news_meta） ====================
        news_items = soup.select(
            '.news_list.list2 .news, ul.news_list.list2 li.news, .news_list.list2 li, .news_list .news')
        if news_items:
            current = []
            for li in news_items:
                a = li.select_one('.news_title a') or li.select_one('a.news_title') or li.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 提取日期（.news_meta 是关键）
                date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .news_list.list2 .news 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：.notice_list + .date (day + year) ====================
        notice_items = soup.select('.notice_list li')
        if notice_items:
            current = []
            for li in notice_items:
                a = li.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .notice_list 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：.jsdetailsred（含 [日期] + a标题） ====================
        js_items = soup.select('.jsdetailsred')
        if js_items:
            current = []
            for item in js_items:
                a = item.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 提取日期
                date_text = self._extract_date_from_element(item)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .jsdetailsred 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：.viewItem.both（含 span 日期） ====================
        view_items = soup.select('.viewItem.both, .viewItem')
        if view_items:
            current = []
            for item in view_items:
                a = item.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期在 span
                date_text = self._extract_date_from_element(item)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .viewItem.both 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：.macnulli + span.libiao 结构 ====================
        mac_items = soup.select('.macnulli li')
        if mac_items:
            current = []
            for li in mac_items:
                a = li.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期在 span.libiao
                date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .macnulli + .libiao 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：.list_right_news（含 span 日期 + h1 a 标题） ====================
        news_items = soup.select('.list_right_news')
        if news_items:
            current = []
            for item in news_items:
                a = item.select_one('h1 a') or item.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期在第一个 span
                date_text = self._extract_date_from_element(item)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .list_right_news 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：.introductionItem + .numberAndDate 结构 ====================
        intro_items = soup.select('.introductionItem')
        if intro_items:
            current = []
            for item in intro_items:
                a = item.select_one('a.noMargin') or item.find('a', href=True)
                if not a:
                    continue

                # 标题
                title_elem = item.select_one('.title')
                title = title_elem.get_text(strip=True) if title_elem else a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期
                date_text = self._extract_date_from_element(item)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .introductionItem + .numberAndDate 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：.listBox + .time (b + p) 结构 ====================
        listbox_items = soup.select('.listBox a')
        if listbox_items:
            current = []
            for a in listbox_items:
                title_elem = a.select_one('h6')
                title = title_elem.get_text(strip=True) if title_elem else a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 提取日期
                date_text = self._extract_date_from_element(a)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .listBox + .time (b+p) 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：.noticeListDetail + .commonA + span 日期 ====================
        detail_items = soup.select('.noticeListDetail')
        if detail_items:
            current = []
            for detail in detail_items:
                a = detail.select_one('a.commonA') or detail.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期在 span
                date_text = self._extract_date_from_element(detail)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .noticeListDetail + .commonA 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：.list_passage + .list_time 结构 ====================
        passage_items = soup.select('.list_passage')
        if passage_items:
            current = []
            for passage in passage_items:
                a = passage.select_one('.list_title a') or passage.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期
                date_text = self._extract_date_from_element(passage)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .list_passage + .list_time 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 强化版：.contain_list + li[id^="line_u"]（本次重点强化） ====================
        line_items = soup.select('li[id^="line_u"], .contain_list li, .contain_list li[id^="line_u"]')
        if line_items:
            current = []
            for li in line_items:
                a = li.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 8 or title in ['更多', '>>', '首页', '返回', '院务通知']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .contain_list + li[id^='line_u'] 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：.RP_item + .RP_date 结构 ====================
        rp_items = soup.select('.RP_item')
        if rp_items:
            current = []
            for item in rp_items:
                a = item.select_one('.RP_content a') or item.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 提取日期
                date_text = self._extract_date_from_element(item)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .RP_item + .RP_date 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：.list-txt-03 .box-s + .box-date ====================
        list_items = soup.select('ul.list-txt-03 li.box-s, .list-txt-03 .box-s')
        if list_items:
            current = []
            for li in list_items:
                a = li.select_one('a.a-title') or li.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期
                date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 2:  # 此结构条数可能较少，降低阈值
                self.log.emit(f"[成功] .list-txt-03 .box-s 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：.content_1 + .con_t + .con_d 结构 ====================
        content_items = soup.select('.content_1')
        if content_items:
            current = []
            for div in content_items:
                a_title = div.select_one('.con_t a')
                if not a_title:
                    continue

                title = a_title.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a_title.get('href', ''))

                # 日期在 .con_d
                date_text = self._extract_date_from_element(div)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .content_1 + .con_t/.con_d 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：.txtList + time + span 结构 ====================
        txt_list = soup.select('ul.txtList')
        if txt_list:
            current = []
            for ul in txt_list:
                for li in ul.select('li'):
                    a = li.find('a', href=True)
                    if not a:
                        continue

                    # 标题（h4 或 a 文本）
                    h4 = li.select_one('h4')
                    title = h4.get_text(strip=True) if h4 else a.get_text(strip=True)
                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', ''))

                    # 日期提取
                    date_text = self._extract_date_from_element(li)

                    current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .txtList + time span 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：.date_day + .date_yearly 结构 ====================
        containers = soup.select('.col-xl-12[style*="background-color"]')
        if containers:
            current = []
            for container in containers:
                a = container.select_one('a.listnews') or container.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 提取日期
                date_text = self._extract_date_from_element(container)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .date_day + .date_yearly 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：dl + dt（span day + year.month）结构 ====================
        dl_list = soup.select('dl')
        if dl_list:
            current = []
            for dl in dl_list:
                a = dl.select_one('dd h1 a') or dl.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 提取日期（优先 dt 中的 span + 文本）
                date_text = self._extract_date_from_element(dl)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] dl + dt 分割日期策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：.text-list .item + .date.te_c 分割日期 ====================
        item_list = soup.select('.text-list .item, .text-list a.item')
        if item_list:
            current = []
            for item in item_list:
                a = item if item.name == 'a' else item.find('a', href=True)
                if not a:
                    continue

                # 标题
                title_elem = item.select_one('.tit')
                title = title_elem.get_text(strip=True) if title_elem else a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期提取
                date_text = self._extract_date_from_element(item)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .text-list .item 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增/强化：.news_list.list2 .news + .news_meta 类型（你最新提供的结构） ====================
        news_items = soup.select('.news_list.list2 .news, ul.news_list.list2 li.news, .news_list .news')
        if news_items:
            current = []
            for li in news_items:
                a = li.select_one('.news_title a') or li.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 提取日期（.news_meta 中的 year + day）
                date_text = self._extract_date_from_element(li)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .news_list.list2 .news + .news_meta 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：嵌套表格 + white-space:nowrap 日期类型 ====================
        tables = soup.select('table')
        if tables:
            current = []
            for table in tables:
                rows = table.select('tr')
                for tr in rows:
                    tds = tr.find_all('td')
                    if len(tds) < 2:
                        continue

                    # 查找链接（通常在第一个td）
                    a = tds[0].find('a', href=True)
                    if not a:
                        continue

                    title = a.get_text(strip=True)
                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', ''))

                    # 日期在第二个td（white-space:nowrap）
                    date_text = tds[1].get_text(strip=True)
                    if not date_text or not re.search(r'\d', date_text):
                        date_text = self._extract_date_from_element(tr)

                    current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] 嵌套表格 white-space:nowrap 策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增：.row table td 右对齐日期类型（你最新提供的结构） ====================
        row_tables = soup.select('.row table')
        if row_tables:
            current = []
            for table in row_tables:
                tr = table.select_one('tr')
                if not tr:
                    continue
                tds = tr.find_all('td')
                if len(tds) < 2:
                    continue

                a = tds[0].find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 日期在最后一个 td（右对齐）
                date_text = tds[-1].get_text(strip=True)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .row table 右对齐日期策略命中 → {len(current)} 条")
                return current

        # ==================== 1. 新增/优化：.wrap + .news_title2 + .news_time3 类型 ====================
        wrap_items = soup.select('.wrap')
        if wrap_items:
            current = []
            for wrap in wrap_items:
                a = wrap.select_one('.news_title2 a') or wrap.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                # 提取日期（.news_time3）
                date_elem = wrap.select_one('.news_time3')
                date_text = ""
                if date_elem:
                    date_text = date_elem.get_text(strip=True)
                else:
                    date_text = self._extract_date_from_element(wrap)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .wrap .news_time3 结构策略命中 → {len(current)} 条")
                return current

        # news3-list-item 类型
        news3_items = soup.select('.news3-list-item')
        if news3_items:
            current = []
            for item in news3_items:
                a = item.select_one('.news3-list-item-text-title a') or item.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                date_container = item.select_one('.news3-list-item-right-time')
                date_text = ""
                if date_container:
                    day = date_container.select_one('.news3-list-item-right-time-day')
                    year = date_container.select_one('.news3-list-item-right-time-year')
                    if day and year:
                        date_text = f"{year.get_text(strip=True)}-{day.get_text(strip=True)}"
                    else:
                        date_text = date_container.get_text(strip=True)
                else:
                    date_text = self._extract_date_from_element(item)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .news3-list-item 结构策略命中 → {len(current)} 条")
                return current

        # .well + .PostTime 类型
        well_items = soup.select('.well')
        if well_items:
            current = []
            for well in well_items:
                a = well.select_one('.ellipsis a') or well.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))

                post_time = well.select_one('.PostTime')
                date_text = ""
                if post_time:
                    text = post_time.get_text(strip=True)
                    date_text = re.sub(r'发布时间[:：\s]*', '', text).strip()
                else:
                    date_text = self._extract_date_from_element(well)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .well .PostTime 结构策略命中 → {len(current)} 条")
                return current

        # .item + .time 类型
        item_list = soup.select('.item')
        if item_list:
            current = []
            for item in item_list:
                a = item if item.name == 'a' else item.find('a', href=True)
                if not a:
                    continue

                title_elem = item.select_one('.title') or a
                title = title_elem.get_text(strip=True)
                title = re.sub(r'^\d+|\[\s*学院公示\s*\]', '', title).strip()

                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))
                date_elem = item.select_one('.time')
                date_text = date_elem.get_text(strip=True) if date_elem else self._extract_date_from_element(item)
                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .item + .time 结构策略命中 → {len(current)} 条")
                return current

        # .wrap + .news_title2 + .news_time3 类型
        wrap_items = soup.select('.wrap[id^="line_u"]')
        if wrap_items:
            current = []
            for wrap in wrap_items:
                a = wrap.select_one('.news_title2 a') or wrap.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))
                date_elem = wrap.select_one('.news_time3')
                date_text = date_elem.get_text(strip=True) if date_elem else self._extract_date_from_element(wrap)
                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .wrap .news_time3 结构策略命中 → {len(current)} 条")
                return current

        # .xw-date.date11 类型
        xw_items = soup.select('ul li')
        if xw_items:
            current = []
            for li in xw_items:
                a = li.find('a', href=True)
                if not a:
                    continue

                title_elem = a.select_one('.tz-tx h3') or a
                title = title_elem.get_text(strip=True)

                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))
                date_text = self._extract_date_from_element(li)
                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .xw-date.date11 结构策略命中 → {len(current)} 条")
                return current

        # .wListUl 类型
        wlist_items = soup.select('.wListUl li')
        if wlist_items:
            current = []
            for li in wlist_items:
                a = li.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))
                date_span = li.find('span')
                date_text = date_span.get_text(strip=True) if date_span else self._extract_date_from_element(li)
                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .wListUl 结构策略命中 → {len(current)} 条")
                return current

        # .jzlb clearfix 类型
        jzlb_items = soup.select('.jzlb')
        if jzlb_items:
            current = []
            for div in jzlb_items:
                a = div.select_one('.btt3 a') or div.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))
                date_elem = div.select_one('.fbsj4')
                date_text = date_elem.get_text(strip=True) if date_elem else self._extract_date_from_element(div)
                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .jzlb 结构策略命中 → {len(current)} 条")
                return current

        # .news_meta + .news_year + .news_day 类型
        news_meta_items = soup.select('.news_list .news, ul.news_list li.news')
        if news_meta_items:
            current = []
            for li in news_meta_items:
                a = li.select_one('.news_title a') or li.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))
                date_text = self._extract_date_from_element(li)
                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .news_meta + year+day 策略命中 → {len(current)} 条")
                return current

        # 表格 align="right" 日期类型
        tables = soup.select('table table') or soup.select('table')
        if tables:
            current = []
            for table in tables:
                rows = table.select('tr')
                for tr in rows:
                    tds = tr.find_all('td')
                    if len(tds) < 3:
                        continue

                    a = tds[1].find('a', href=True) if len(tds) > 1 else None
                    if not a:
                        continue

                    title = a.get_text(strip=True)
                    if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                        continue

                    url = urljoin(base_url, a.get('href', ''))
                    date_text = ""
                    if len(tds) >= 3:
                        date_text = tds[-1].get_text(strip=True)
                    if not date_text:
                        date_text = self._extract_date_from_element(tr)

                    current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] 表格 align=right 日期策略命中 → {len(current)} 条")
                return current

        # index-notice 结构
        notice_items = soup.select('li:has(.index-notice-top), li:has(.index-notice-bottom)')
        if notice_items:
            current = []
            for li in notice_items:
                a = li.find('a', href=True)
                if not a:
                    continue

                title_elem = a.select_one('.index-notice-top p') or a
                title = title_elem.get_text(strip=True)

                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))
                date_span = a.select_one('.index-notice-bottom span')
                date_text = date_span.get_text(strip=True) if date_span else self._extract_date_from_element(li)
                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .index-notice 结构策略命中 → {len(current)} 条")
                return current

        # .list-art1 类型
        list_art_items = soup.select('.list-art1')
        if list_art_items:
            current = []
            for div in list_art_items:
                a = div.find('a', href=True)
                if not a:
                    continue

                p_title = a.select_one('p')
                title = p_title.get_text(strip=True) if p_title else a.get_text(strip=True)

                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))
                date_span = a.select_one('span')
                date_text = date_span.get_text(strip=True) if date_span else self._extract_date_from_element(div)
                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .list-art1 结构策略命中 → {len(current)} 条")
                return current

        # .newsList .newsDate 类型
        news_list_items = soup.select('.newsList li, ul.newsList li')
        if news_list_items:
            current = []
            for li in news_list_items:
                a = li.select_one('.newsTitle a') or li.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))
                date_elem = li.select_one('.newsDate')
                date_text = date_elem.get_text(strip=True) if date_elem else self._extract_date_from_element(li)
                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .newsList .newsDate 策略命中 → {len(current)} 条")
                return current

        # <i>[03月06日]</i> 类型
        i_items = soup.select('ul li')
        if i_items:
            current = []
            for li in i_items:
                a = li.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))
                i_tag = li.find('i')
                date_text = i_tag.get_text(strip=True) if i_tag else self._extract_date_from_element(li)
                date_text = re.sub(r'[\[\]]', '', date_text).strip()
                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 2:
                self.log.emit(f"[成功] <i>[日期]</i> 结构策略命中 → {len(current)} 条")
                return current

        # 简单 ul > li > a + span 日期类型
        simple_li_items = soup.select('ul li')
        if simple_li_items:
            current = []
            for li in simple_li_items:
                a = li.find('a', href=True)
                if not a:
                    continue

                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', ''))
                span = li.find('span')
                date_text = span.get_text(strip=True) if span else self._extract_date_from_element(li)
                date_text = re.sub(r'[\s\xa0\u3000]+', '', date_text).strip()
                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] 简单 ul li + span 日期策略命中 → {len(current)} 条")
                return current

        # .childBox 类型
        childbox_items = soup.select('.childBox')
        if childbox_items:
            current = []
            for box in childbox_items:
                a = box.find('a', href=True)
                if not a:
                    continue
                title_elem = a.select_one('.contentRight span') or a
                title = title_elem.get_text(strip=True) if title_elem else a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue
                url = urljoin(base_url, a.get('href', ''))
                date_text = self._extract_date_from_element(box)
                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .childBox 策略命中 → {len(current)} 条")
                return current

        # .tx-date 类型
        tx_items = soup.select('li.wow.fadeInUp')
        if tx_items:
            current = []
            for li in tx_items:
                a = li.find('a', href=True)
                if not a:
                    continue
                title = a.select_one('h3') or a
                title_text = title.get_text(strip=True) if hasattr(title, 'get_text') else a.get_text(strip=True)
                if len(title_text) < 6:
                    continue
                url = urljoin(base_url, a.get('href', ''))
                date_text = self._extract_date_from_element(li)
                current.append({"title": title_text, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .tx-date 策略命中 → {len(current)} 条")
                return current

        # .li2 + .d2 类型
        li2_items = soup.select('.li2')
        if li2_items:
            current = []
            for div in li2_items:
                a = div.select_one('.w2 a') or div.find('a', href=True)
                if not a:
                    continue
                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue
                url = urljoin(base_url, a.get('href', ''))
                date_text = self._extract_date_from_element(div)
                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] .li2 + .d2 策略命中 → {len(current)} 条")
                return current

        # 其他已有结构
        line_items = soup.select('li[id^="line_u"], li[id^="line_u8"], li[id^="line_u9"]')
        if line_items:
            current = []
            for li in line_items:
                a = li.find('a', href=True)
                if not a:
                    continue
                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue
                url = urljoin(base_url, a.get('href', ''))
                date_text = self._extract_date_from_element(li)
                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] li[id^='line_u'] 策略命中 → {len(current)} 条")
                return current

        # 通用策略
        strategies = [
            "ul.list-gl li", "ul.news li", "ul.notice_list li", ".news-list li",
            ".notice-list li", ".list-item", ".news-item", ".wp_article_list li",
            ".content-list li", ".list_content li", ".list_box li", "li.news",
            "li[class*='news']", ".article-list li", ".info-list li",
            "dl.list dd", "table tr", 'b.rt', 'b', '.news_time'
        ]

        for selector in strategies:
            elements = soup.select(selector)
            if len(elements) < 3:
                continue
            if elements and any(e.find('th') for e in elements[:2]):
                elements = elements[1:]

            current = []
            for elem in elements:
                a = elem.find('a', href=True)
                if not a:
                    continue
                title = a.get_text(strip=True)
                if len(title) < 6 or title in ['更多', '>>', '首页', '返回']:
                    continue
                url = urljoin(base_url, a['href'])
                date_text = self._extract_date_from_element(elem)
                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self.log.emit(f"[成功] 通用结构化列表策略命中 → {len(current)} 条")
                return current

        # 最终兜底
        candidate_items = []
        for a in soup.find_all('a', href=True):
            title = a.get_text(strip=True)
            href = a.get('href', '').strip()
            if len(title) < 8 or href.startswith(('javascript', '#')):
                continue
            if any(k in title for k in ['首页', '上页', '下页', '尾页', '登录', '更多>>']):
                continue
            full_url = urljoin(base_url, href)
            date_text = self._extract_date_from_element(a)
            candidate_items.append({"title": title, "url": full_url, "date": date_text})

        seen = set()
        for item in candidate_items:
            if item['title'] not in seen:
                seen.add(item['title'])
                items.append(item)

        if len(items) >= 3:
            self.log.emit(f"[成功] 纯链接兜底策略 → {len(items)} 条")
            return items[:80]

        self.log.emit(f"[警告] 自动识别较弱，仅提取 {len(items)} 条")
        return items

    def _detect_inline_title_date(self, html, base_url):
        soup = BeautifulSoup(html, 'html.parser')
        items = []
        for elem in soup.find_all(['li', 'div', 'tr', 'dd', 'p']):
            a = elem.find('a', href=True)
            if not a:
                continue
            title = a.get_text(strip=True)
            if len(title) < 6:
                continue
            url = urljoin(base_url, a['href'])
            date_text = self._extract_date_from_element(elem)
            items.append({"title": title, "url": url, "date": date_text})
        if items:
            self.log.emit(f"[信息] 行内标题日期模式 ({len(items)} 条)")
        return items

    def _auto_detect_table(self, html, base_url):
        soup = BeautifulSoup(html, 'html.parser')
        for table in soup.find_all('table'):
            rows = table.find_all('tr')
            if len(rows) < 3:
                continue
            data_rows = rows[1:] if rows and rows[0].find('th') else rows

            items = []
            for row in data_rows:
                a = row.find('a', href=True)
                if not a:
                    continue
                title = a.get_text(strip=True)
                if len(title) < 5:
                    continue
                url = urljoin(base_url, a['href'])
                date_text = self._extract_date_from_element(row)
                items.append({"title": title, "url": url, "date": date_text})

            if len(items) >= 3:
                self.log.emit(f"[信息] Table结构 ({len(items)} 条)")
                return items
        return []

    # ====================== 日期处理 ======================
    def _extract_date_from_element(self, element):
        """
        超级增强版日期提取 - 终极优化与精炼版 (已修正 select_one 参数 Bug)
        采用高容错、四阶段优先级对齐算法，彻底根除脏数据与重合误杀。
        """
        if not element:
            return ""

        # ==================== 【南京信息工程大学 最终强力版】 ====================
        html_str = str(element)

        # 方案1：精确匹配 <a>...</a><span>日期</span>
        match = re.search(
            r'<a[^>]*>.*?</a>\s*<span[^>]*?>\s*(\d{4}-\d{1,2}-\d{1,2})\s*</span>',
            html_str,
            re.DOTALL | re.IGNORECASE
        )
        if match:
            return match.group(1)

        # 方案2：任意位置的 span 日期
        match2 = re.search(
            r'<span[^>]*?>\s*(\d{4}-\d{1,2}-\d{1,2})\s*</span>',
            html_str,
            re.IGNORECASE
        )
        if match2:
            return match2.group(1)

        # 方案3：超级兜底 - 在整个文本中找标准日期格式
        match3 = re.search(r'\b(\d{4}-\d{1,2}-\d{1,2})\b', html_str)
        if match3:
            return match3.group(1)

        # 6. 通用标签内 YYYY-MM-DD 正则匹配（包含所有 span 和自定义样式）
        for tag in ['span', 'div', 'p', 'li', 'td', 'time']:
            for item in element.find_all(tag):
                text = item.get_text(strip=True)
                # 增加匹配捕获组
                match = re.search(r'(\d{4}-\d{1,2}-\d{1,2})', text)
                if match:
                    return match.group(1)  # 修正：此处由原来的 return text 修改为仅返回匹配到的日期部分

        # 新增：合并拆分在 .time 内部的 span (年-月，如 2026.04) 与 em (天，如 17) 标签内的日期
        time_div = element.select_one('.time')
        if time_div:
            span_ym = time_div.select_one('span')
            em_day = time_div.select_one('em')
            if span_ym and em_day:
                year_month = span_ym.get_text(strip=True).replace('/', '-').replace('.', '-').strip()
                day = em_day.get_text(strip=True).strip()
                # 验证并合并为标准格式 YYYY-MM-DD
                if re.match(r'^\d{4}[-.]\d{1,2}$', year_month) and day.isdigit():
                    return f"{year_month}-{day.zfill(2)}"

        # 新增：合并拆分在 .date 内部的 .date2 (年-月，如 2025-11) 与 .date1 (天，如 13) 标签内的日期
        date_div = element.select_one('.date')
        if date_div:
            d1_day = date_div.select_one('.date1')
            d2_ym = date_div.select_one('.date2')
            if d1_day and d2_ym:
                year_month = d2_ym.get_text(strip=True).replace('/', '-').replace('.', '-').strip()
                day = d1_day.get_text(strip=True).strip()
                # 验证并格式化提取出的日期构造段
                if re.match(r'^\d{4}[-.]\d{1,2}$', year_month) and day.isdigit():
                    return f"{year_month}-{day.zfill(2)}"

        # 合并拆分在 span.time1 内部的 b (天，如 03) 与其余文本 (年-月，如 2026-04) 标签内的日期
        span_time1 = element.select_one('.time1')
        if span_time1:
            b_day = span_time1.select_one('b')
            if b_day:
                day = b_day.get_text(strip=True)
                full_text = span_time1.get_text(strip=True)
                year_month = full_text.replace(day, "").replace('/', '-').replace('.', '-').strip()
                if re.match(r'^\d{4}[-.]\d{1,2}$', year_month) and day.isdigit():
                    return f"{year_month}-{day.zfill(2)}"

        # 新增：合并拆分在 .tm 内部的 span (年份，如 2026) 与 p (月-日，如 06-01) 标签内的日期
        tm_div = element.select_one('.tm')
        if tm_div:
            span_year = tm_div.select_one('span')
            p_day = tm_div.select_one('p')
            if span_year and p_day:
                year = span_year.get_text(strip=True)
                month_day = p_day.get_text(strip=True).replace('/', '-').replace('.', '-').strip()
                # 验证提取到的日期段是否属于合法结构
                if re.match(r'^\d{4}$', year) and re.match(r'^\d{1,2}[-.]\d{1,2}$', month_day):
                    return f"{year}-{month_day}"

        # 新增：合并拆分在 .time 内部的两个 p 标签 (第一个为天数，第二个为年-月) 标签内的日期
        time_div = element.select_one('.time')
        if time_div:
            p_tags = time_div.find_all('p')
            if len(p_tags) >= 2:
                day = p_tags[0].get_text(strip=True)
                year_month = p_tags[1].get_text(strip=True).replace('/', '-').replace('.', '-').strip()
                # 验证提取到的各部分格式是否属于合法日期构造
                if day.isdigit() and re.match(r'^\d{4}[-.]\d{1,2}$', year_month):
                    return f"{year_month}-{day.zfill(2)}"

        # 新增：合并拆分在 span.time1 内部的 b (天，如 05) 与其余文本 (年-月，如 2024-01) 标签内的日期
        span_time1 = element.select_one('.time1')
        if span_time1:
            b_day = span_time1.select_one('b')
            if b_day:
                day = b_day.get_text(strip=True)
                full_text = span_time1.get_text(strip=True)
                year_month = full_text.replace(day, "").replace('/', '-').replace('.', '-').strip()
                # 验证提取到的日期段属于合法结构
                if re.match(r'^\d{4}[-.]\d{1,2}$', year_month) and day.isdigit():
                    return f"{year_month}-{day.zfill(2)}"

        # 新增：合并拆分在 .time 内部的 span.s2 (年.月，如 2024.04) 与 span.s1 (天，如 03) 标签内的日期
        time_div = element.select_one('.time')
        if time_div:
            s1_day = time_div.select_one('.s1')
            s2_ym = time_div.select_one('.s2')
            if s1_day and s2_ym:
                year_month = s2_ym.get_text(strip=True).replace('/', '-').replace('.', '-').strip()
                day = s1_day.get_text(strip=True).strip()
                # 验证提取到的日期段是否属于合法结构
                if re.match(r'^\d{4}[-.]\d{1,2}$', year_month) and day.isdigit():
                    return f"{year_month}-{day.zfill(2)}"

        # 新增：合并拆分在 span.flex 内部的 i (天，如 02) 与其余文本 (年-月，如 2024-12) 标签内的日期
        span_flex = element.select_one('span.flex')
        if span_flex:
            i_tag = span_flex.select_one('i')
            if i_tag:
                day = i_tag.get_text(strip=True)
                full_text = span_flex.get_text(strip=True)
                year_month = full_text.replace(day, "").replace('/', '-').replace('.', '-').strip()
                # 验证提取到的日期段是否属于合法结构
                if re.match(r'^\d{4}[-.]\d{1,2}$', year_month) and day.isdigit():
                    return f"{year_month}-{day.zfill(2)}"

        # 新增：合并拆分在 .tim 内部的 span (年-月，如 2026-03) 与 .day (日，如 25) 标签内的日期
        tim_div = element.select_one('.tim')
        if tim_div:
            span_ym = tim_div.select_one('span')
            day_div = tim_div.select_one('.day')
            if span_ym and day_div:
                year_month = span_ym.get_text(strip=True).replace('/', '-').replace('.', '-').strip()
                day = day_div.get_text(strip=True).strip()
                # 验证提取到的日期段属于合法结构
                if re.match(r'^\d{4}[-.]\d{1,2}$', year_month) and day.isdigit():
                    return f"{year_month}-{day.zfill(2)}"

        # 新增：合并拆分在 .con_zt_cen_nero_lb_rl 内部的 span (年-月，如 2025-09) 与 strong (天，如 19) 标签内的日期
        rl_div = element.select_one('.con_zt_cen_nero_lb_rl')
        if rl_div:
            span_ym = rl_div.select_one('span')
            strong_day = rl_div.select_one('strong')
            if span_ym and strong_day:
                year_month = span_ym.get_text(strip=True).replace('/', '-').replace('.', '-').strip()
                day = strong_day.get_text(strip=True).strip()
                # 验证提取到的日期段是否属于合法结构
                if re.match(r'^\d{4}[-.]\d{1,2}$', year_month) and day.isdigit():
                    return f"{year_month}-{day.zfill(2)}"

        # 新增：合并拆分在 .date 内部的 span (年份，如 2025) 与 b (月/日，如 10/13) 标签内的日期 [1]
        date_div = element.select_one('.date')
        if date_div:
            span_year = date_div.select_one('span')
            b_day = date_div.select_one('b')
            if span_year and b_day:
                year = span_year.get_text(strip=True)
                month_day = b_day.get_text(strip=True).replace('/', '-').replace('.', '-').strip()
                # 验证提取到的日期段属于合法结构
                if re.match(r'^\d{4}$', year) and re.match(r'^\d{1,2}[-.]\d{1,2}$', month_day):
                    return f"{year}-{month_day}"

        # 新增：合并拆分在 .time 内部的 em (年-月，如 2023-06) 与 b (日，如 30) 标签内的日期
        time_div = element.select_one('.time')
        if time_div:
            b_day = time_div.select_one('b')
            em_year_month = time_div.select_one('em')
            if b_day and em_year_month:
                day = b_day.get_text(strip=True)
                year_month = em_year_month.get_text(strip=True).replace('/', '-').replace('.',
                                                                                          '-').strip()
                # 验证提取到的日期段是否属于合法结构
                if re.match(r'^\d{4}[-.]\d{1,2}$', year_month) and day.isdigit():
                    return f"{year_month}-{day.zfill(2)}"

        # 6. 通用标签内 YYYY-MM-DD 正则匹配（在标签列表中新增 'small'）
        for tag in ['span', 'div', 'p', 'li', 'td', 'time', 'small']:
            for item in element.find_all(tag):
                text = item.get_text(strip=True)
                if re.search(r'\d{4}-\d{1,2}-\d{1,2}', text):
                    return text

        # 新增：合并拆分在 .item-date 内部的 .date (年-月，如 2026-05) 与 .day (日，如 19) 标签内的日期
        item_date_div = element.select_one('.item-date')
        if item_date_div:
            ym_span = item_date_div.select_one('.date')
            day_span = item_date_div.select_one('.day')
            if ym_span and day_span:
                ym_val = ym_span.get_text(strip=True).replace('/', '-').replace('.', '-')
                day_val = day_span.get_text(strip=True).strip()
                # 校验格式，确保重组为标准 YYYY-MM-DD
                if re.match(r'^\d{4}[-.]\d{1,2}$', ym_val) and day_val.isdigit():
                    return f"{ym_val}-{day_val.zfill(2)}"

        # 新增：合并拆分在 .month (年-月，如 2026-03) 与 .day (日，如 24) 标签内的日期
        month_elem = element.select_one('.month')
        day_elem = element.select_one('.day')
        if month_elem and day_elem:
            month_val = month_elem.get_text(strip=True).replace('/', '-').replace('.', '-')
            day_val = day_elem.get_text(strip=True).strip()
            # 校验格式，确保能够重组为标准 YYYY-MM-DD 格式
            if re.match(r'^\d{4}[-.]\d{1,2}$', month_val) and day_val.isdigit():
                return f"{month_val}-{day_val.zfill(2)}"

        # 新增：合并拆分在 .yy (年份) 与 .mmdd (月-日) 标签内的日期
        yy_elem = element.select_one('.yy')
        mmdd_elem = element.select_one('.mmdd')
        if yy_elem and mmdd_elem:
            year = yy_elem.get_text(strip=True)
            month_day = mmdd_elem.get_text(strip=True).replace('/', '-').replace('.', '-')
            if re.match(r'^\d{4}$', year) and re.match(r'^\d{1,2}[-.]\d{1,2}$', month_day):
                return f"{year}-{month_day}"

        # 新增：山大商学院 .zt-ul-date 结构（span 包含月-日，font 包含年份）
        # 能够秒级合流 YYYY-MM 与 DD 并用标准连字符对齐
        zt_date = element.select_one('.zt-ul-date')
        if zt_date:
            span = zt_date.select_one('span')
            font = zt_date.select_one('font')
            if span and font:
                day_month = span.get_text(strip=True).replace('/', '-')
                year = font.get_text(strip=True)
                if day_month and year:
                    return f"{year}-{day_month}"

        # 新增：合并拆分在 .date span (年份，如 2026) 与 .date 外部文本 (月-日，如 03-27) 标签内的日期
        date_div = element.select_one('.date')
        if date_div:
            span_year = date_div.select_one('span')
            if span_year:
                year = span_year.get_text(strip=True)
                full_text = date_div.get_text(strip=True)
                month_day = full_text.replace(year, "", 1).replace('/', '-').replace('.', '-').strip()
                if re.match(r'^\d{4}$', year) and re.match(r'^\d{1,2}[-.]\d{1,2}$', month_day):
                    return f"{year}-{month_day}"

        # 新增：合并拆分在 .year (年份) 与 .day (月.日，如 02.23) 标签内的日期
        year_elem = element.select_one('.year')
        day_elem = element.select_one('.day')
        if year_elem and day_elem:
            year = year_elem.get_text(strip=True)
            month_day = day_elem.get_text(strip=True).replace('.', '-').replace('/', '-')
            if re.match(r'^\d{4}$', year) and re.match(r'^\d{1,2}[-.]\d{1,2}$', month_day):
                return f"{year}-{month_day}"

        # 新增：北师大生命/通用 .date-item 结构（.ym 包含年.月，.d 包含日）
        # 能够秒级合流 YYYY.MM 与 DD，并用标准连字符对齐
        date_item = element.select_one('.date-item')
        if date_item:
            ym = date_item.select_one('.ym')
            d = date_item.select_one('.d')
            if ym and d:
                day = d.get_text(strip=True)
                year_month = ym.get_text(strip=True).replace('.', '-')
                if day and year_month:
                    # 补零并合并为标准格式 YYYY-MM-DD，如 "2026-05" + "26" -> "2026-05-26"
                    return f"{year_month}-{day.zfill(2)}"

        # 新增：合并拆分在 d1 (年份) 与 d2 (月-日) 标签内的日期
        d1_elem = element.select_one('.d1')
        d2_elem = element.select_one('.d2')
        if d1_elem and d2_elem:
            year = d1_elem.get_text(strip=True)
            month_day = d2_elem.get_text(strip=True).replace('/', '-').replace('.', '-')
            if re.match(r'^\d{4}$', year) and re.match(r'^\d{1,2}[-.]\d{1,2}$', month_day):
                return f"{year}-{month_day}"

        # 新增：针对 <span><i>19</i>2026-03</span> 的自适应合并逻辑
        span_i = element.select_one('span')
        if span_i and span_i.select_one('i'):
            i_tag = span_i.select_one('i')
            day = i_tag.get_text(strip=True)
            full_text = span_i.get_text(strip=True)  # "192026-03"
            if day:
                year_month = full_text.replace(day, "", 1).strip('- \t\n\r/')
                if day.isdigit() and re.match(r'^\d{4}[-/]\d{1,2}$', year_month):
                    return f"{year_month}-{day.zfill(2)}"

        # 新增：从标签自带的 date 属性中直接提取标准日期（如 <div class="item_day" date="2026-05-18">）
        for attr_elem in element.find_all(attrs={"date": True}):
            attr_val = attr_elem.get('date', '').strip()
            # 校验是否为标准的日期格式（支持 -, /, . 作为分隔符）
            if re.match(r'^\d{4}[-/.]\d{1,2}[-/.]\d{1,2}$', attr_val):
                return attr_val

        date_item = element.select_one('.date-item, .events-item .date')
        if date_item:
            ym = date_item.select_one('.ym, .year-month, .date-y-m')
            d = date_item.select_one('.d, .day, .date-day')
            if ym and d:
                day_str = d.get_text(strip=True)
                ym_str = ym.get_text(strip=True)

                # 清洗年月部分
                ym_clean = re.sub(r'[^\d.\-/]', '', ym_str).strip()
                ym_clean = ym_clean.replace('.', '-').replace('/', '-')

                if day_str.isdigit() and re.search(r'\d{4}', ym_clean):
                    # 支持 "2026.05" -> "2026-05-26"
                    parts = ym_clean.split('-')
                    if len(parts) == 2:
                        year, month = parts
                        if len(year) == 4 and month.isdigit():
                            return f"{year}-{month.zfill(2)}-{day_str.zfill(2)}"

        # 能够秒级合流 YYYY.MM 与 DD，并用标准连字符对齐
        date_item = element.select_one('.date-item')
        if date_item:
            ym = date_item.select_one('.ym')
            d = date_item.select_one('.d')
            if ym and d:
                day = d.get_text(strip=True)
                year_month = ym.get_text(strip=True).replace('.', '-')
                if day and year_month:
                    # 补零并合并为标准格式 YYYY-MM-DD，如 "2026-05" + "26" -> "2026-05-26"
                    return f"{year_month}-{day.zfill(2)}"

        # 1. 复杂分拆式日期结构重组 (具有强排他性)

        # 新增：华南理工 .datetime-box 结构（.datetime-day 包含日，.datetime-month 包含年-月）
        # 升级点：精准识别并合并 YYYY-MM + DD
        datetime_box = element.select_one('.datetime-box')
        if datetime_box:
            day_span = datetime_box.select_one('.datetime-day')
            month_span = datetime_box.select_one('.datetime-month')
            if day_span and month_span:
                day = day_span.get_text(strip=True)
                year_month = month_span.get_text(strip=True).replace('/', '-')
                if day and year_month:
                    # 补零并强制拼合为标准 YYYY-MM-DD，如 "2025-12" + "03" -> "2025-12-03"
                    return f"{year_month}-{day.zfill(2)}"

        # 华南理工/通用 .li-date 或 .li-data 结构（p 包含日，span 包含年-月）
        li_date = element.select_one('.li-date, .li-data')
        if li_date:
            p = li_date.select_one('p')
            span = li_date.select_one('span')
            if p and span:
                day = p.get_text(strip=True)
                year_month = span.get_text(strip=True).replace('/', '-').replace('.', '-')
                if day and year_month:
                    return f"{year_month}-{day.zfill(2)}"

        # text-ldata 结构： <p>05/21</p> <span>2026</span>
        ldata = element.select_one('.text-ldata')
        if ldata:
            p = ldata.select_one('p')
            span = ldata.select_one('span')
            if p and span:
                return f"{span.get_text(strip=True)}-{p.get_text(strip=True).replace('/', '-')}"

        # d-newsU5 结构 / 陕科大 / 华北科等：div.date > div.t1 + div.t2
        # 或 flex 结构中的 div.date（span + p）或者 b + span
        # 升级点：高容错兼容吉林大学、哈尔滨音乐、天职师大等各异构分拆式日期结构，并引入智能位置与格式核验机制
        date_div = element.select_one('.date')
        if date_div:
            t1 = date_div.select_one('.t1')  # 月-日
            t2 = date_div.select_one('.t2')  # / 年份
            span = date_div.select_one('span')
            p = date_div.select_one('p')
            b = date_div.select_one('b')
            i_tag = date_div.select_one('i')
            p_tags = date_div.find_all('p')  # 检测行内是否存在多个 p 标签

            if t1 and t2:
                return f"{t2.get_text(strip=True).replace('/', '').strip()}-{t1.get_text(strip=True)}"
            elif span and p:
                span_text = span.get_text(strip=True)
                p_text = p.get_text(strip=True)
                # 智能自适应判定：检测 span 和 p 哪一个是包含 4 位年份的年-月，哪一个是天数，防止拼接颠倒
                if re.search(r'\d{4}', span_text):
                    year_month = span_text.replace('.', '-')
                    day = p_text
                else:
                    year_month = p_text.replace('.', '-')
                    day = span_text
                return f"{year_month}-{day.zfill(2)}"
            elif len(p_tags) >= 2:
                # 模式：第 1 个 p 为天数，第 2 个 p 为年-月
                day = p_tags[0].get_text(strip=True)
                year_month = p_tags[1].get_text(strip=True).replace('/', '-')
                if day and year_month:
                    return f"{year_month}-{day.zfill(2)}"
            elif span and b:
                return f"{span.get_text(strip=True).strip()}-{b.get_text(strip=True).strip().zfill(2)}"
            elif span and i_tag:
                # 哈音等结构：i 为天，span 为年-月
                span_text = span.get_text(strip=True)
                i_text = i_tag.get_text(strip=True)
                if re.search(r'\d{4}', span_text):
                    year_month = span_text.replace('.', '-')
                    day = i_text
                else:
                    year_month = i_text.replace('.', '-')
                    day = span_text
                return f"{year_month}-{day.zfill(2)}"
            elif span:
                # ==================== 【最新新增】：内嵌 4 位年份 span + 外部日/月的分拆式结构 ====================
                year = span.get_text(strip=True)
                if re.match(r'^\d{4}$', year):
                    full_text = date_div.get_text(strip=True)
                    # 剥离出裸露的月日部分
                    date_part = full_text.replace(year, '').strip()
                    # 确定提取的格式为 MM-DD 形式
                    if re.match(r'^\d{1,2}[-/.]\d{1,2}$', date_part):
                        return f"{year}-{date_part.replace('.', '-')}"

        # specialTime 结构： <span>2026</span> 05-08
        special_time = element.select_one('.specialTime')
        if special_time:
            span_year = special_time.find('span')
            year = span_year.get_text(strip=True) if span_year else ''
            date_part = special_time.get_text(strip=True).replace(year, '').strip()
            if year and date_part:
                return f"{year}-{date_part.replace('-', '-')}"

        # div.time 结构（<div class="day"> + <div class="year">）
        # 或 .posttime 中山大学 list-page（.day + .year-month）
        # 或 .notice_list 中的 .date（.day + .year）
        time_div = element.select_one('.time, .posttime, .date')
        if time_div:
            day = time_div.select_one('.day, strong, b')
            year = time_div.select_one('.year, .year-month, p')
            if day and year:
                day_text = day.get_text(strip=True)
                year_text = year.get_text(strip=True).replace('/', '-').strip()
                # 兼容英文月份映射 (如 Apr 10)
                month_map = {
                    'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
                    'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
                    'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
                }
                if month_map.get(day_text[:3].lower()):
                    month = month_map.get(day_text[:3].lower())
                    return f"{datetime.now().year}-{month}-{year_text.zfill(2)}"
                # 正常合并
                if '-' in year_text:
                    return f"{year_text}-{day_text.zfill(2)}"
                return f"{year_text}-{day_text}"

        # 学工、研工、兰理工、杭高院等 time em + p 结构快速识别
        time_tag = element.select_one('time')
        if time_tag:
            em = time_tag.select_one('em')
            p = time_tag.select_one('p')
            span = time_tag.select_one('span')
            if em and p:
                day = em.get_text(strip=True)
                year_month = p.get_text(strip=True).replace('/', '-')
                if day and year_month:
                    return f"{year_month}-{day.zfill(2)}"
            elif span:
                md = span.get_text(strip=True)
                year = time_tag.get_text(strip=True).replace(md, "").strip()
                if year and md:
                    return f"{year}-{md}"

        # dl dt 结构（<span>19</span> 2026.05）
        dt = element.select_one('dt')
        if dt:
            span = dt.select_one('span')
            if span:
                day = span.get_text(strip=True)
                year_month = dt.get_text(strip=True).replace(day, "").strip()
                if year_month and day:
                    return f"{year_month}-{day}"

        # .date.te_c 结构（2026.05 + <span>08</span>）
        date_div = element.select_one('.date.te_c')
        if date_div:
            text = date_div.get_text(" ", strip=True)
            span = date_div.select_one('span')
            if span:
                day = span.get_text(strip=True)
                year_month = text.replace(day, "").strip()
                if year_month and day:
                    return f"{year_month}-{day}"

        # .news_year + .news_day
        year_elem = element.select_one('.news_year')
        day_elem = element.select_one('.news_day')
        if year_elem and day_elem:
            year_month = year_elem.get_text(strip=True).replace('/', '-').strip()
            day = day_elem.get_text(strip=True).strip()
            if year_month and day:
                return f"{year_month}-{day.zfill(2)}"

        # contentChild 结构
        content_child = element.select_one('.contentChild')
        if content_child:
            spans = content_child.find_all('span')
            if len(spans) >= 2:
                month_day = spans[0].get_text(strip=True)
                year = spans[-1].get_text(strip=True)
                if month_day and year and re.search(r'\d', year):
                    return f"{year}-{month_day}"

        # tx-date 结构
        tx_date = element.select_one('.tx-date')
        if tx_date:
            spans = tx_date.find_all('span')
            b = tx_date.select_one('b')
            if spans and b:
                month_day = spans[0].get_text(strip=True)
                year = b.get_text(strip=True)
                if month_day and year:
                    return f"{year}-{month_day}"

        # xw-date 结构
        xw_date = element.select_one('.xw-date')
        if xw_date:
            b = xw_date.select_one('b')
            span = xw_date.select_one('span')
            if b and span:
                day = b.get_text(strip=True)
                month_year = span.get_text(strip=True)
                if day and month_year:
                    return f"{month_year}-{day}"

        # 2. 精准类/标签属性提取

        # 中山大学 list-content / 详情页中的各类 span.date / time 标签合并解析
        # 兼容 Bootstrap card 中的 p.text-muted time
        time_tag = element.select_one('time')
        if time_tag:
            dt_attr = time_tag.get('datetime')
            if dt_attr:
                return dt_attr
            text = time_tag.get_text(strip=True)
            if re.search(r'\d{4}', text):
                return text

        # announcements-list 中的 .announcement-time
        time_div = element.select_one('.announcement-time')
        if time_div:
            md = time_div.select_one('.md')
            y = time_div.select_one('.y')
            if md and y:
                return f"{y.get_text(strip=True)}-{md.get_text(strip=True)}"
            text = time_div.get_text(strip=True)
            if re.search(r'\d', text):
                return text

        # p.time 结构（<span>04-16</span>2026）
        time_p = element.select_one('p.time')
        if time_p:
            span = time_p.select_one('span')
            if span:
                month_day = span.get_text(strip=True)
                year = time_p.get_text(strip=True).replace(month_day, "").strip()
                if year and month_day:
                    return f"{year}-{month_day}"

        # .numberAndDate 结构（.number + .date）
        num_date = element.select_one('.numberAndDate')
        if num_date:
            date_elem = num_date.select_one('.date')
            if date_elem:
                text = date_elem.get_text(strip=True)
                if re.search(r'\d', text):
                    return text

        # 3. 高优特征类名快速匹配列表
        selectors = [
            '.fbsj4', '.news_meta', '.news_time', '.article-date', '.cols_meta',
            '.date', 'span.date', '.news_day', '.d2', '.d1', '.newsDate', '.time',
            '.PostTime', '.news_time3', '.libiao', '.blog-rt-box-item-rt',
            '.column-news-date', '.nDate', '.main-left-time', '.date2', '.con-li-nr .time',
            '.list_date', 'a[style*="float:right"]', 'a[style*="float: right"]', '.dateR',
            '.subTongzhiTextItemDate', '.sub_list_date', '.subListTime', '.time_all', '.news-info',
            '.title-en', '.pull-right', '.list-time', '.lzd-blog-date', '.gpArticleDate', '.newdate',
            '.list_ljgd', '.fbdate', '.time-12', '.column-news-date', '.date',
            '.list-columnsTime', '.columnNew-time', '.line2', '.ca_news_ym', '.ny-list', '.alld',
            'small', '.sj', '.ni-date', 'em', '.list-date', '.info', '.subpage-list-item span', '.article-item-info',
            '.column-news-date',
        ]
        # 去重并保持顺序
        seen_sel = set()
        unique_selectors = [x for x in selectors if not (x in seen_sel or seen_sel.add(x))]

        for sel in unique_selectors:
            date_elem = element.select_one(sel)
            if date_elem:
                text = date_elem.get_text(strip=True)
                if re.search(r'\d', text):
                    return text

        # 4. 特殊内联样式 / 特有行内节点检测
        # float_right
        float_right = element.select_one('span[style*="float:right"], span[style*="float: right"]')
        if float_right:
            text = float_right.get_text(strip=True)
            if re.search(r'\d{4}', text):
                return text

        # width:19%
        width_span = element.select_one('span[style*="width:19%"], span[style*="19%"]')
        if width_span:
            text = width_span.get_text(strip=True)
            if re.search(r'\d{4}', text):
                return text

        # color:#d0c2a3
        span_date = element.select_one('span[style*="color:#d0c2a3"]')
        if span_date:
            text = span_date.get_text(strip=True)
            if re.search(r'\d{4}', text):
                return text

        # n_list + line_u9_ 结构：日期直接跟在 </a> 后面
        if element.name == 'li' and 'line_u' in element.get('id', ''):
            # 尝试提取 a 标签后面的日期部分
            date_match = re.search(r'</a>\s*([\d]{4}[-/]\d{1,2}[-/]\d{1,2})', str(element))
            if date_match:
                return date_match.group(1)

        # ==================== 第二阶段：中等优先级通用标签内含标准格式匹配 ====================

        # 1. 优先在 a 标签内第一个 span 作为日期（北邮等多校传统结构）
        if element.name == 'a' or element.find('a'):
            first_span = element.select_one('span')
            if not first_span and element.name == 'a':
                # 寻找同级兄弟 span
                for sibling in element.find_next_siblings('span'):
                    text = sibling.get_text(strip=True)
                    if re.search(r'\d{4}', text):
                        first_span = sibling
                        break
            if first_span:
                text = first_span.get_text(strip=True)
                if re.search(r'\d{4}', text):
                    return text

        # ==================== 【最新修复点：父级文本消除法】 ====================
        # 针对 <a> 标签外部直接裸露纯文本日期（如：</a>2026-05-26，成都体育学院等）的非标结构
        # 通过在父级节点中抠除 <a> 标签自身文本，精准、零污染地利用正则提取纯净日期
        if element.name == 'a' and element.parent:
            parent_text = element.parent.get_text(" ", strip=True)
            a_text = element.get_text(" ", strip=True)
            # 消除标题
            remaining_text = parent_text.replace(a_text, "").strip()
            # 净化物理干扰字符
            remaining_text = re.sub(r'[\s\xa0\u3000\n\r|｜\[\]\(\)（）>›»▪•*◎：:：\-]+', ' ',
                                    remaining_text).strip()
            # 用正则从净化后的残留文本中捞取 YYYY MM DD
            m_full = re.search(r'(\d{4})[ -](\d{1,2})[ -](\d{1,2})', remaining_text)
            if m_full:
                return f"{m_full.group(1)}-{m_full.group(2).zfill(2)}-{m_full.group(3).zfill(2)}"

        # 2. 通用标签扫描：寻找带有 [YYYY-MM-DD] 格式或标准时间格式的 span 节点
        for span in element.find_all('span'):
            text = span.get_text(strip=True)
            # 处理 [2026-05-21] 格式
            m_bracket = re.search(r'\[(\d{4}-\d{1,2}-\d{1,2})\]', text)
            if m_bracket:
                return m_bracket.group(1)
            # 处理普通 2026-05-21 格式
            if re.search(r'\d{4}-\d{1,2}-\d{1,2}', text):
                return text

        # 3. 针对 <p><a>...</a><span>日期</span></p> 结构
        if element.name == 'p':
            # 检查同级 span
            for sibling in element.find_next_siblings('span'):
                text = sibling.get_text(strip=True)
                if re.search(r'\d{4}', text):
                    return text
            # 检查 p 内部的 span
            for span in element.find_all('span'):
                text = span.get_text(strip=True)
                if re.search(r'\d{4}', text):
                    return text

        # 4. p 标签内 a 的兄弟 span
        if element.name == 'p' or element.select_one('a'):
            for span in element.find_all('span', recursive=False):
                text = span.get_text(strip=True)
                if re.search(r'\d{4}', text):
                    return text

        # 5. 优先查找兄弟 <span>（针对北航等结构，包含 dd 和任意带有 span 的嵌套行）
        # 【双重防御升级】：如果传入的是 a 标签本身，且它内部无 span，则自动向其同级右侧寻找兄弟 span 节点作为日期
        if element.name == 'a' or element.find('a'):
            first_span = element.select_one('span')
            if not first_span and element.name == 'a':
                # 寻找同级兄弟 span
                for sibling in element.find_next_siblings('span'):
                    text = sibling.get_text(strip=True)
                    if re.search(r'\d{4}', text):
                        first_span = sibling
                        break
            if first_span:
                text = first_span.get_text(strip=True)
                if re.search(r'\d{4}', text):
                    return text

        # 6. 通用标签内 YYYY-MM-DD 正则匹配（包含所有 span 和自定义样式）
        for tag in ['span', 'div', 'p', 'li', 'td', 'time']:
            for item in element.find_all(tag):
                text = item.get_text(strip=True)
                if re.search(r'\d{4}-\d{1,2}-\d{1,2}', text):
                    return text

        # ==================== 第三阶段：低优先级全文特征模式匹配 ====================

        # 1. 尝试对当前元素全文提取，执行精细的 YYYY-MM-DD 或 YYYY年MM月DD日 匹配
        text = element.get_text(" ", strip=True)
        patterns = [
            r'(\d{4}[-/年.]\d{1,2}[-/月.]\d{1,2})',
            r'(\d{4}[-/]\d{1,2}[-/]\d{1,2})',
            r'(\d{4}年\d{1,2}月\d{1,2}日?)',
            r'\[(\d{4}[-/]\d{1,2}[-/]\d{1,2})\]',
            r'(\d{1,2}月\d{1,2}日?)',
        ]
        for pat in patterns:
            m = re.search(pat, text)
            if m:
                return m.group(1)

        # 2. 尝试从父元素提取全文并检索时间
        if element.parent:
            parent_text = element.parent.get_text(" ", strip=True)
            for pat in patterns:
                m = re.search(pat, parent_text)
                if m:
                    return m.group(1)

        # ==================== 第四阶段：最低优先级普通文本兜底提取年份 ====================

        # 兜底：寻找类似 2026 或 2026年 的前四位数字，组装为年度第一天（防止有些行完全没写日期，但标题带有年份）
        year_match = re.search(r'(\d{4})年?', text)
        if year_match:
            return year_match.group(1) + "-01-01"

        return ""

    def _parse_date(self, date_text):
        """超级智能日期解析
        返回: (格式化后的日期 YYYY-MM-DD, 相对今天的具体天数, 是否成功解析)
        """
        if not date_text:
            return "", 999, False

        original = str(date_text).strip()

        # ==================== 【特异性模式 B 拦截与合并重组】 ====================
        # 专门拦截并智能重组类似 "03-292024"、"03-122024"、"11-242023" 等年份在最末尾且与天数无分隔符的非标合并文本 [1]
        # 解构模式 B：月份 (\d{1,2}) + 分隔符 + 天数 (\d{1,2}) + 年份 (\d{4})
        m_merged_b = re.search(r'\b(\d{1,2})[-/.](\d{1,2})(\d{4})\b', original)
        if m_merged_b:
            try:
                month = m_merged_b.group(1).zfill(2)
                day = m_merged_b.group(2).zfill(2)
                year = m_merged_b.group(3)

                # 校验提取出的年月日有效性，如有效则直接拼装返回
                dt = datetime(int(year), int(month), int(day))
                now = datetime.now()
                # 跨年超前防线处理
                if (dt - now).days > 2:
                    try:
                        dt = dt.replace(year=dt.year - 1)
                    except ValueError:
                        dt = dt.replace(year=dt.year - 1, day=28)
                days_old = max(0, (now - dt).days)
                self.log.emit(f"[日期解析成功-特异末尾合并] {original} → {dt.strftime('%Y-%m-%d')} ({days_old}天前)")
                return dt.strftime("%Y-%m-%d"), days_old, True
            except:
                pass

        # 新增：拦截并转换非标中文月份格式（如 "十月 04, 2022" -> "2022-10-04"）
        m_cn_month = re.search(r'([一二三四五六七八九十]+)月\s*(\d{1,2})[,\s]+(\d{4})', original)
        if m_cn_month:
            cn_month_map = {
                "一": "01", "二": "02", "三": "03", "四": "04", "五": "05", "六": "06",
                "七": "07", "八": "08", "九": "09", "十": "10", "十一": "11", "十二": "12"
            }
            month_str = cn_month_map.get(m_cn_month.group(1))
            if month_str:
                day_str = m_cn_month.group(2).zfill(2)
                year_str = m_cn_month.group(3)
                original = f"{year_str}-{month_str}-{day_str}"

        if re.match(r'^\d{4}-\d{2}-\d{2}$', original.strip()):
            try:
                dt = datetime.strptime(original.strip(), "%Y-%m-%d")
                days_old = max(0, (datetime.now() - dt).days)
                return original.strip(), days_old, True
            except:
                pass

        # ==================== 【终极防线：特异非标格式强力拦截】 ====================
        # 专门拦截并重组类似 "202604 月09"、"202604月01" 或 "2026 04月09" 等因合并提取导致的非标文本
        m_gp = re.search(r'(\d{4})[-_./\s]?(\d{1,2})\s*月\s*(\d{1,2})', original)
        if m_gp:
            try:
                year = int(m_gp.group(1))
                month = int(m_gp.group(2))
                day = int(m_gp.group(3))
                dt = datetime(year, month, day)
                now = datetime.now()
                # 只有当解析出的时间超前未来 2 天以上时，才倒扣 1 年（处理无年份跨年问题）
                if (dt - now).days > 2:
                    try:
                        dt = dt.replace(year=dt.year - 1)
                    except ValueError:
                        dt = dt.replace(year=dt.year - 1, day=28)
                days_old = max(0, (now - dt).days)
                self.log.emit(f"[日期解析成功-特异拦截] {original} → {dt.strftime('%Y-%m-%d')} ({days_old}天前)")
                return dt.strftime("%Y-%m-%d"), days_old, True
            except:
                pass

        # ==================== 原有通用清洗与标准解析逻辑 ====================
        # 强力清理中文及符号，保留合理空格与英文冒号，确保 parser.parse 能处理标准日期格式
        cleaned = re.sub(r'[年月]', '-', original)
        cleaned = re.sub(r'[日]', ' ', cleaned)
        cleaned = re.sub(r'[：：]', ':', cleaned)
        cleaned = re.sub(r'[-/.]+', '-', cleaned)
        # 去除圆括号、中括号等，并将多个空格压缩为一个
        cleaned = re.sub(r'[\[\]\(\)（）\xa0\u3000]+', ' ', cleaned)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip('- \t\n\r')

        # 1. dateutil 智能解析优先（处理整理后的干净字符串）
        try:
            dt = parser.parse(cleaned, fuzzy=True, yearfirst=True)
            # 只有当解析出的时间超前未来 2 天以上时，才倒扣 1 年（处理无年份短日期的跨年问题）
            if (dt - now).days > 2:
                try:
                    dt = dt.replace(year=dt.year - 1)
                except ValueError:  # 应对闰年 2 月 29 日
                    dt = dt.replace(year=dt.year - 1, day=28)
            days_old = max(0, (now - dt).days)  # 微弱的时差时，days_old 强制归 0
            self.log.emit(f"[日期解析成功] {original} → {dt.strftime('%Y-%m-%d')} ({days_old}天前)")
            return dt.strftime("%Y-%m-%d"), days_old, True
        except:
            pass

        # 处理 2025.1204 格式的非标准书写
        m_dot = re.search(r'(\d{4})\.(\d{2})(\d{2})', cleaned)
        if m_dot:
            cleaned = f"{m_dot.group(1)}-{m_dot.group(2)}-{m_dot.group(3)}"

        # 2. 正则表达式兜底提取（修正在 cleaned 变量上进行匹配，而不是 original）
        patterns = [
            r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})',
            r'(\d{4})(\d{2})(\d{2})',
        ]

        for pat in patterns:
            m = re.search(pat, cleaned)
            if m:
                try:
                    year = int(m.group(1))
                    month = int(m.group(2))
                    day = int(m.group(3))
                    if year < 100:
                        year += 2000
                    dt = datetime(year, month, day)
                    now = datetime.now()
                    if (dt - now).days > 2:
                        try:
                            dt = dt.replace(year=dt.year - 1)
                        except ValueError:
                            dt = dt.replace(year=dt.year - 1, day=28)
                    days_old = max(0, (now - dt).days)
                    return dt.strftime("%Y-%m-%d"), days_old, True
                except:
                    continue

        # 3. 处理无年份的短日期，如 "05-10" 或 "5-10"
        short_pattern = r'(\d{1,2})[-/](\d{1,2})'
        m_short = re.search(short_pattern, cleaned)
        if m_short:
            try:
                month = int(m_short.group(1))
                day = int(m_short.group(2))
                dt = datetime(now.year, month, day)
                # 拼接今年后如果是未来时间，说明实际是去年的公告
                if (dt - now).days > 2:
                    try:
                        dt = dt.replace(year=now.year - 1)
                    except ValueError:
                        dt = dt.replace(year=now.year - 1, day=28)
                days_old = max(0, (now - dt).days)
                return dt.strftime("%Y-%m-%d"), days_old, True
            except:
                pass

        self.log.emit(f"[日期解析失败] 原始文本: {original}")
        # 解析完全失败时返回 False
        return original, 999, False

    # ====================== 智能混合日期对齐辅助方法 ======================

    def _find_link_node(self, soup, base_url, link):
        """精准/模糊双通道定位列表页的 a 标签"""
        target_abs_url = urljoin(base_url, link) if link else ''
        if not target_abs_url:
            return None

        # 1. 绝对路径精密比对
        for possible_a in soup.find_all('a', href=True):
            possible_href = possible_a.get('href', '').strip()
            if possible_href.startswith(('javascript:', '#', 'mailto:')):
                continue
            if urljoin(base_url, possible_href) == target_abs_url:
                return possible_a

        # 2. 备用文件名后缀比对
        target_filename = link.split('/')[-1] if '/' in link else link
        if target_filename and len(target_filename) > 5:
            for possible_a in soup.find_all('a', href=True):
                possible_href = possible_a.get('href', '').strip()
                if target_filename in possible_href:
                    return possible_a
        return None

    def _find_single_row_container(self, a_tag, base_url):
        """向上爬升寻找安全的单行局部容器，防止侵入全局外壳"""
        curr = a_tag
        parent_container = None
        for _ in range(5):
            if curr is None or curr.name == 'body':
                break

            # 单行约束过滤：如果这个容器内部包含了 2 个及以上不同的文章链接，说明已经到了全局大容器，应当停止爬升
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

    def _resolve_local_date(self, parent_container, title, extract_datetime_func):
        """本地回溯提取与分裂日期合并核心协调器"""
        if not parent_container:
            return "", False

        # 策略 1：依靠传统消除标题法检索
        temp_date = self._extract_date_for_gerapy_item(parent_container, title)
        if temp_date and (
                re.search(r'\d{4}[-/年.]\d{1,2}[-/月.]\d{1,2}', temp_date) or re.match(r'^\d{8}$', temp_date)):
            return temp_date, True

        container_text = parent_container.get_text(" ", strip=True)

        # 策略 2：通过分裂日期格式解析器提取并重构
        reconstructed = self._extract_split_date(container_text)
        if reconstructed:
            return reconstructed, True

        # 策略 3：基于类名特征启发式合并
        class_split = self._resolve_split_date_from_container(parent_container)
        if class_split:
            return class_split, True

        # 策略 4：使用 gerapy 的 extract_datetime 提取局部 HTML 的特征
        if extract_datetime_func:
            try:
                raw_extracted = extract_datetime_func(str(parent_container))
                if raw_extracted:
                    if not isinstance(raw_extracted, str):
                        try:
                            extracted_str = raw_extracted.strftime('%Y-%m-%d %H:%M:%S')
                        except:
                            extracted_str = str(raw_extracted)
                    else:
                        extracted_str = raw_extracted

                    reconstructed = self._extract_split_date(extracted_str)
                    return (reconstructed if reconstructed else extracted_str), True
            except:
                pass

        return "", False

    def _extract_split_date(self, text):
        """清洗文本并按特征模型融合成标准 YYYY-MM-DD"""
        if not text:
            return None
        cleaned_text = re.sub(r'\s+', ' ', text)

        # 模式 1：标准连贯格式
        m1 = re.search(r'\b(20\d{2})\s*[-/年.]\s*([01]?\d)\s*[-/月.]\s*([0-3]?\d)\s*日?\b', cleaned_text)
        if m1:
            return f"{m1.group(1)}-{int(m1.group(2)):02d}-{int(m1.group(3)):02d}"

        # 模式 2：年份与月日分裂如 "2021 06-15"
        m2 = re.search(r'\b(20\d{2})\s+([01]?\d)[-/.]?([0-3]?\d)\b', cleaned_text)
        if m2:
            return f"{m2.group(1)}-{int(m2.group(2)):02d}-{int(m2.group(3)):02d}"

        # 模式 3：月日在前，年份在后如 "06-15 2021"
        m3 = re.search(r'\b([01]?\d)[-/.]?([0-3]?\d)\s+(20\d{2})\b', cleaned_text)
        if m3:
            return f"{m3.group(3)}-{int(m3.group(1)):02d}-{int(m3.group(2)):02d}"

        # 模式 4：年月在前，日在后如 "2021-06 15"
        m4 = re.search(r'\b(20\d{2})[-/.](\d{1,2})\s+([0-3]?\d)\b', cleaned_text)
        if m4:
            return f"{m4.group(1)}-{int(m4.group(2)):02d}-{int(m4.group(3)):02d}"

        # 模式 5：日在前，年月在后如 "15 2021-06"
        m5 = re.search(r'\b([0-3]?\d)\s+(20\d{2})[-/.](\d{1,2})\b', cleaned_text)
        if m5:
            return f"{m5.group(2)}-{int(m5.group(3)):02d}-{int(m5.group(1)):02d}"

        return None

    def _resolve_split_date_from_container(self, container):
        """启发式类名与结构分析，融合您的多个分裂日期 bs4 手工规则"""
        if not container:
            return None

        year_val, month_val, day_val = None, None, None

        # 遍历子元素中带有特定排版含义的类名
        for elem in container.find_all(True):
            cls = "".join(elem.get('class', [])).lower()
            text = elem.get_text(strip=True)
            if not text:
                continue

            # A. 提取年份、年月信息 (匹配类名含 year, yy, s2, ym, monthyear, date2 等)
            if any(x in cls for x in ['year', 'yy', 's2', 'ym', 'monthyear', 'date2']):
                m_yr = re.search(r'\b(20\d{2})\b', text)
                if m_yr:
                    year_val = m_yr.group(1)
                m_ym = re.search(r'\b(20\d{2})[-/.](\d{1,2})\b', text)
                if m_ym:
                    year_val = m_ym.group(1)
                    month_val = m_ym.group(2)

            # B. 提取日期、日份信息 (匹配类名含 day, dd, d1, riqi, time1, s1, md 等)
            if any(x in cls for x in ['day', 'dd', 'd1', 'riqi', 'time1', 's1', 'md']):
                m_day = re.search(r'\b([0-3]?\d)\b', text)
                if m_day:
                    day_val = m_day.group(1)
                m_md = re.search(r'\b([01]?\d)[-/]([0-3]?\d)\b', text)
                if m_md:
                    month_val = m_md.group(1)
                    day_val = m_md.group(2)

        # 组装校验
        if year_val and month_val and day_val:
            try:
                return f"{year_val}-{int(month_val):02d}-{int(day_val):02d}"
            except ValueError:
                pass
        elif year_val and day_val:
            # 兼容 year_val 自带年月（如 2024-06 或 2024.06）的形式
            cleaned_ym = year_val.replace('.', '-').replace('/', '-')
            if re.match(r'^\d{4}-\d{1,2}$', cleaned_ym):
                return f"{cleaned_ym}-{int(day_val):02d}"

        return None

    def _probe_detail_page(self, url, extract_datetime_func):
        """向目标详情页发送探测，优先通过 extract_datetime 解析"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            # verify=False 规避大部分高校的老旧 SSL 证书报错问题
            resp = requests.get(url, headers=headers, timeout=6, verify=False)
            resp.encoding = resp.apparent_encoding or 'utf-8'
            detail_html = resp.text
            if not detail_html:
                return None

            # 1. 尝试使用 gerapy 的 extract_datetime 提取
            if extract_datetime_func:
                try:
                    raw_detail_date = extract_datetime_func(detail_html)
                    if raw_detail_date:
                        if isinstance(raw_detail_date, datetime):
                            return raw_detail_date.strftime('%Y-%m-%d %H:%M:%S')
                        return str(raw_detail_date)
                except:
                    pass

            # 2. 备用：尝试使用 htmldate 提取
            try:
                from htmldate import find_date
                detail_date = find_date(detail_html)
                if detail_date:
                    return detail_date
            except ImportError:
                pass
        except Exception as e:
            self.log.emit(f"[详情页探测异常] 请求失败: {e}")
        return None

    def _check_notice(self, notice):
        """关键词过滤 + 时间过滤"""
        # ==================== 时间过滤 ====================
        if self.days > 0:
            if notice.days_old > self.days and notice.days_old < 900:  # 900是解析失败的标记
                self.log.emit(f"[时间过滤] 排除：{notice.title} ({notice.days_old}天前 > {self.days}天)")
                return False
            else:
                self.log.emit(f"[时间过滤] 通过：{notice.title} ({notice.days_old}天前)")

        # ==================== 关键词过滤 ====================
        if self.include_keywords:
            matched_keywords = [k for k in self.include_keywords if k in notice.title]
            if not matched_keywords:
                return False
            notice.keywords = ", ".join(matched_keywords)
        else:
            notice.keywords = "全部"

        for k in self.exclude_keywords:
            if k in notice.title:
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
        """执行脚本"""
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
        """执行单个脚本并收集结果"""
        import runpy
        import os
        import sys

        # 创建结果收集器
        collected_results = []

        def emit_result(data):
            """脚本内部调用此函数来提交结果"""
            if isinstance(data, dict):
                collected_results.append(data)
                self.result.emit(data)
            else:
                self.log.emit(f"[警告] 脚本返回了无效数据格式: {type(data)}")

        # 准备脚本执行环境
        script_globals = {
            '__name__': '__main__',
            '__file__': script_path,
            'emit_result': emit_result,  # 提供给脚本使用的函数
            'log': lambda msg: self.log.emit(f"[脚本日志] {msg}"),
        }

        # 将脚本所在目录添加到路径
        script_dir = os.path.dirname(os.path.abspath(script_path))
        if script_dir not in sys.path:
            sys.path.insert(0, script_dir)

        try:
            # 执行脚本
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
