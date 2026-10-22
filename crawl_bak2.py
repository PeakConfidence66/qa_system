"""
爬虫模块 - 支持 xpath/css/regex + POST+JSON 接口
优化版：模块化重构自动识别引擎、高容错分裂日期重组与多线程并发
"""
import re
import json
import threading
from datetime import datetime
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from curl_cffi import requests as curl_requests
from bs4 import BeautifulSoup
from lxml import etree
from dateutil import parser
from PyQt6.QtCore import QThread, pyqtSignal

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

    def _recalc_current_dates(self, current_list):
        """辅助函数：为列表元素统一计算 days_old 和标准格式化时间，防止时区误差"""
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
                        self.log.emit(f"[错误] 链接处理失败 {url}: {str(e)}")

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

            if resp is not None and 200 <= resp.status_code < 300:
                return self._process_response(resp)

        except Exception as e:
            self.log.emit(f"[普通requests失败] {method} {url} -> {str(e)}")

        # 兜底：使用 curl_cffi
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
            return self._process_response(resp)
        except Exception as e:
            self.log.emit(f"[curl_cffi也失败] {method} {url} -> {str(e)}")
            return None

    def _process_response(self, resp):
        """统一处理响应编码"""
        if resp is None:
            return None

        encoding = None
        if hasattr(resp, 'apparent_encoding') and resp.apparent_encoding:
            encoding = resp.apparent_encoding

        if not encoding or encoding.lower() in ['iso-8859-1', 'ascii']:
            content_type = resp.headers.get('Content-Type', '')
            if 'charset=' in content_type:
                try:
                    encoding = content_type.split('charset=')[-1].strip().lower()
                except:
                    pass

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
        return resp

    # ====================== 翻页主逻辑 ======================
    def _crawl_with_pagination(self, start_url, school_name, college_name, rules):
        """带翻页采集 - 优化去重逻辑与时间拦截"""
        current_url = start_url
        page_count = 0
        total_items = 0
        seen_urls = set()
        seen_titles_this_run = set()
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

                if title in seen_titles_this_run and page_count > 1:
                    continue

                seen_urls.add(link)
                seen_titles_this_run.add(title)
                new_count += 1

                publish_date, days_old, is_valid_date = self._parse_date(date_text)

                if self.days > 0:
                    if not is_valid_date:
                        self.log.emit(f"[跳过] 无法解析日期 ({date_text})，在时间限制下安全过滤该公告")
                        continue

                    if days_old > self.days:
                        consecutive_old += 1
                        if consecutive_old >= 5:
                            self.log.emit(f"[停止] 连续 {consecutive_old} 条公告超出 {self.days} 天范围，终止本项抓取")
                            should_stop_pagination = True
                            break
                        else:
                            self.log.emit(
                                f"[跳过] 超出天数范围 ({days_old}天前)，疑似置顶或解析偏差，继续检查后续公告...")
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
                    with self._lock:
                        self.total_after_filter += 1
                    page_items += 1

            if should_stop_pagination:
                break

            total_items += page_items
            self.log.emit(f"[本页] 新增有效内容 {new_count} 条，入库 {page_items} 条")

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
            self.log.emit(f"[警告] XPath解析失败: {str(e)}")
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
            self.log.emit(f"[警告] CSS解析失败: {str(e)}")
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
            self.log.emit(f"[警告] Regex解析失败: {str(e)}")
        return items

    # ====================== 自动识别重构核心 ======================
    def _auto_detect_parse(self, html, base_url):
        """三级分流自适应提取引擎"""
        self.log.emit(f"[自动识别] 启动解析: {base_url}")

        # 1. 优先采用 Gerapy_auto_extractor 提取并进行邻近 DOM 智能时间对齐
        items = self._extract_via_gerapy_alignment(html, base_url)
        if items and len(items) >= 3:
            self.log.emit(f"[对齐成功] Gerapy 提取器搭配邻近对齐机制成功提取 {len(items)} 条结构化数据")
            return items

        # 2. 备选方案一：提取包含于 <script> 标签内的 JSON 数据（如 allData, dataList 嵌入格式）
        items = self._parse_script_json_embeds(html, base_url)
        if items and len(items) >= 3:
            return items

        # 3. 备选方案二：使用高度概括的一组经典 DOM 卡片特征匹配
        items = self._parse_handcrafted_layouts(html, base_url)
        if items and len(items) >= 1:
            return items

        # 4. 终极兜底：纯链接和文本标签遍历解析
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
            self.log.emit("[警告] gerapy_auto_extractor 未安装，跳过智能自动提取")
        except Exception as e:
            self.log.emit(f"[Gerapy 自动提取分支异常] {e}")
        return []

    # ====================== 脚本JSON解析分支 ======================
    def _parse_script_json_embeds(self, html, base_url):
        """解析页面 Script 标签中嵌入的 JSON 列表（如 dataList 或 allData 格式）"""
        soup = BeautifulSoup(html, 'html.parser')
        script_tags = soup.find_all('script')
        current = []

        for script in script_tags:
            script_text = script.get_text()
            # 1. 匹配 dataList 模式
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
                            data_list = json.loads(script_text[bracket_start:bracket_end + 1])
                            for block in data_list:
                                for info in block.get('infolist', []):
                                    title = (info.get('title') or info.get('infotitle') or '').strip()
                                    link = (info.get('url') or info.get('sourceurl') or '').strip()
                                    if len(title) < 4:
                                        continue

                                    # 读取时间
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
                            self.log.emit(f"[警告] 解析 dataList 嵌入JSON失败: {ex}")

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
            self.log.emit(f"[成功] 命中内资 Script JSON 嵌入式解析机制，采集 {len(current)} 条")
            return current
        return []

    # ====================== 经典 DOM 结构自适应重构 ======================
    def _parse_handcrafted_layouts(self, html, base_url):
        """采用通用性的复合布局配置解析非标及拆分样式页面"""
        soup = BeautifulSoup(html, 'html.parser')
        current = []

        # 将几十个高校繁琐的小规则合并成高度可配置、高度概括的四大主要经典布局
        configs = [
            {
                # 规则 1：经典的 split-date 复合卡片（适用于东华、鲁东、清华研招、温州医大等）
                "selector": ".item, div.item, .newstitem, .column-news-item, .article-item, .subpage-list-item, .notice-item, a.list-item, .ej_list2 li, .subNoticeList li, .news-item",
                "title_sel": "h2, p.title, .column-news-title, .article-item-title, p, h3, .ni-title",
                "link_sel": "a",
                "date_sel": ".date, .column-news-date, .article-item-info, span, .ni-date, p.sj"
            },
            {
                # 规则 2：列表行带有 line_uX 命名或 id 的经典结构（适用于陕师大、长安、西工大、吉大等）
                "selector": "li[id^='line_u'], dl[id^='line_u'], tr[id^='line_u'], li.wow",
                "title_sel": ".ltrlb_tit, h3, h2, a, .titline",
                "link_sel": "a",
                "date_sel": ".time1, .time, span, time"
            },
            {
                # 规则 3：表格及 Joomla 分类行（适用于各类高校表格、Joomla 默认模板）
                "selector": "tr[class*='cat-list-row'], table.wp_article_list_table tr, table tr:has(a)",
                "title_sel": ".list-title a, a",
                "link_sel": "a",
                "date_sel": ".list-date, td[align='right'], td"
            }
        ]

        for cfg in configs:
            nodes = soup.select(cfg["selector"])
            if not nodes:
                continue

            current = []
            for node in nodes:
                # 尝试提取 a 标签链接
                a = node if node.name == 'a' else node.find('a', href=True)
                if not a or not a.get('href') or any(p in a.get('href', '').lower() for p in ['javascript:', '#']):
                    continue

                # 提取标题（优先使用特定标题选择器）
                title_node = node.select_one(cfg["title_sel"]) if cfg["title_sel"] else None
                title = title_node.get_text(strip=True) if title_node else a.get('title', '').strip() or a.get_text(
                    strip=True)
                title = re.sub(r'[…\s\.]+$', '', title).strip()

                if len(title) < 4 or title in ['更多', '>>', '首页', '返回']:
                    continue

                url = urljoin(base_url, a.get('href', '').strip())

                # 先尝试利用邻近裂变重组器拼合散落在微观 DOM 中的日期（针对分裂布局）
                date_text = self._extract_split_date_from_container(node, title)
                if not date_text and cfg["date_sel"]:
                    # 若拼合失败，读取通用配置元素
                    date_node = node.select_one(cfg["date_sel"])
                    date_text = date_node.get_text(strip=True) if date_node else self._extract_date_from_element(node)

                current.append({"title": title, "url": url, "date": date_text})

            if len(current) >= 3:
                self._recalc_current_dates(current)
                self.log.emit(f"[成功] 命中高度概括性布局规则群, 采集 {len(current)} 条")
                return current

        # 处理个别极为特殊、无标准 A 链接的跳转（如西安交通大学Marx、首师大 checkXnwk、清华 yzbm data-val、长春理工 onclick）
        # 通过在 DOM 级搜寻含有跳转特征的自定义事件直接进行事件级破译
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
                # 解析 checkXnwk(file, ...) 或者是 window.open(url) 等
                match_fn = re.search(r"(?:checkXnwk|window\.open|location\.href|location)\(\s*['\"](.*?)['\"]",
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
                self.log.emit(f"[成功] 动态跳转事件破译成功, 提取 {len(current)} 条")
                return current

        return []

    # ====================== 智能微观分裂日期重组器 ======================
    def _extract_split_date_from_container(self, container, title):
        """
        微观 DOM 邻近分裂日期重组器
        剔除标题文字干扰，智能寻找并将物理截断的 [年-月] 与 [日] 或 [月-日] 与 [年] 合并输出为标准 YYYY-MM-DD。
        """
        if not container:
            return None

        # 提取容器内非标题文本节点，避免标题数字造成干扰
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
            self.log.emit(f"[成功] 纯链接无结构化兜底策略匹配成功，采集 {len(items)} 条")
            return items[:80]

        self.log.emit(f"[警告] 自动识别均未完全吻合，仅提取 {len(items)} 条")
        return items

    # ====================== 日期清洗与解析逻辑 ======================
    def _extract_date_from_element(self, element):
        """通用高容错日期文本提取"""
        if not element:
            return ""

        # 1. 尝试直接匹配标准 YYYY-MM-DD
        html_str = str(element)
        match_std = re.search(r'\b(\d{4}-\d{1,2}-\d{1,2})\b', html_str)
        if match_std:
            return match_std.group(1)

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
        高度兼容性时间解析器
        返回：(格式化日期 YYYY-MM-DD, 相对天数, 是否成功解析)
        """
        if not date_text:
            return "", 999, False

        original = str(date_text).strip()
        now = datetime.now()

        # 处理非标末尾年份黏连格式（如 "05-212024"）
        m_merged = re.search(r'\b(\d{1,2})[-/.](\d{1,2})(\d{4})\b', original)
        if m_merged:
            try:
                dt = datetime(int(m_merged.group(3)), int(m_merged.group(1)), int(m_merged.group(2)))
                if (dt - now).days > 2:
                    dt = dt.replace(year=dt.year - 1)
                days_old = max(0, (now - dt).days)
                return dt.strftime("%Y-%m-%d"), days_old, True
            except:
                pass

        # 基础格式格式化清洗
        cleaned = re.sub(r'[年月]', '-', original)
        cleaned = re.sub(r'[日]', ' ', cleaned)
        cleaned = re.sub(r'[-/.]+', '-', cleaned)
        cleaned = re.sub(r'[\[\]\(\)（）\xa0\u3000]+', ' ', cleaned)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip('- \t\n\r')

        # 使用 dateutil 工具智能解析
        try:
            dt = parser.parse(cleaned, fuzzy=True, yearfirst=True)
            if (dt - now).days > 2:
                try:
                    dt = dt.replace(year=dt.year - 1)
                except ValueError:
                    dt = dt.replace(year=dt.year - 1, day=28)
            days_old = max(0, (now - dt).days)
            return dt.strftime("%Y-%m-%d"), days_old, True
        except:
            pass

        # 正则兜底解析
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
                    if (dt - now).days > 2:
                        dt = dt.replace(year=dt.year - 1)
                    days_old = max(0, (now - dt).days)
                    return dt.strftime("%Y-%m-%d"), days_old, True
                except:
                    continue

        # 无年份短日期解析（如 05-23）
        m_short = re.search(r'(\d{1,2})[-/](\d{1,2})', cleaned)
        if m_short:
            try:
                dt = datetime(now.year, int(m_short.group(1)), int(m_short.group(2)))
                if (dt - now).days > 2:
                    dt = dt.replace(year=now.year - 1)
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

            # 约束：如向上攀爬过程中容器中出现两个及以上详情 A 链接，说明越过了行级范围
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

            # 1. 尝试使用 gerapy extract_datetime
            try:
                from gerapy_auto_extractor import extract_datetime
                raw_detail_date = extract_datetime(detail_html)
                if raw_detail_date:
                    if isinstance(raw_detail_date, datetime):
                        return raw_detail_date.strftime('%Y-%m-%d %H:%M:%S')
                    return str(raw_detail_date)
            except:
                pass

            # 2. 尝试使用 htmldate
            try:
                from htmldate import find_date
                detail_date = find_date(detail_html)
                if detail_date:
                    return detail_date
            except ImportError:
                pass
        except Exception as e:
            self.log.emit(f"[详情页探测异常] 访问失败: {e}")
        return None

    def _check_notice(self, notice):
        """关键词及时间双向过滤"""
        if self.days > 0:
            if notice.days_old > self.days and notice.days_old < 900:
                self.log.emit(f"[时间过滤] 排除：{notice.title} ({notice.days_old}天前 > {self.days}天)")
                return False
            else:
                self.log.emit(f"[时间过滤] 通过：{notice.title} ({notice.days_old}天前)")

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