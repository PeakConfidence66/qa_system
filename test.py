# # my_spider.py
# import requests
# from bs4 import BeautifulSoup
#
# url = "https://yzbm.tsinghua.edu.cn/publish/s03/s0301/list"
# prefix = "https://yzbm.tsinghua.edu.cn/publish/s03/s0301/detail/"
# headers = {
#     "User-Agent": "Mozilla/5.0",
#     "Content-Type": "application/x-www-form-urlencoded",
# }
# cookies = {
#     "cn_com_southsoft_gms": "85f44cd3-8544-4f28-9fbe-c7885ca6abc3"
# }
#
# def scrape_announcements(page_no=1, page_size=20):
#     """抓取指定页的公告并调用 emit_result 提交"""
#     data = {"pageNo": page_no, "pageSize": page_size}
#     resp = requests.post(url, headers=headers, cookies=cookies, data=data)
#     resp.encoding = "utf-8"
#     soup = BeautifulSoup(resp.text, "lxml")
#
#     count = 0
#     for li in soup.select("ul#content li"):
#         a_tag = li.find("a")
#         if not a_tag:
#             continue
#         title = li.select_one("div.name")
#         title = title.get_text(strip=True) if title else ""
#         date = li.select_one("div.time")
#         date = date.get_text(strip=True) if date else ""
#         data_val = a_tag.get("data-val")
#         if not data_val:
#             continue
#         link = prefix + data_val
#
#         # 使用 emit_result 提交单条数据
#         emit_result({
#             "title": title,           # 标题（必填）
#             "url": link,              # 链接（必填）
#             "publish_date": date,     # 发布日期
#             "school": "清华大学",      # 学校名称
#             "category": "研究生院",    # 类别
#             "keywords": "博士招生",    # 关键词
#             "days_old": 0             # 时效（天），这里可以自己计算
#         })
#         count += 1
#
#     print(f"第 {page_no} 页采集完成，共采集 {count} 条")
#     return count > 0  # 如果这一页有数据，返回 True，否则 False
#
# def run_spider():
#     """主函数，遍历分页直到没有数据"""
#     page_no = 1
#     while True:
#         has_data = scrape_announcements(page_no)
#         if not has_data:
#             break
#         page_no += 1
#     print("采集任务完成")
#
# # 入口
# if __name__ == "__main__":
#     run_spider()


# from gerapy_auto_extractor import extract_list
# from parsel import Selector
# import requests
#
# url = "https://sis.wmu.edu.cn/zw24/xydt/tzgg/1.htm"
# headers = {
#                 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
#             }
# resp = requests.get(url,headers=headers, timeout=10)
# resp.encoding = resp.apparent_encoding
# print(resp.text)
# # 提取标题和链接
# items = extract_list(resp.text)
#
# # 用 parsel 提取日期
# sel = Selector(text=resp.text)
# dates = sel.css("ul.list li span.time::text").getall()
#
# # 合并结果
# results = []
# for i, item in enumerate(items):
#     title = item.get("title")
#     link = item.get("url")
#     date = dates[i] if i < len(dates) else None
#     results.append({
#         "标题": title,
#         "链接": link,
#         "日期": date
#     })
#
# # 打印结果
# for r in results:
#     print(r)


