# from scrapling.fetchers import DynamicSession
# from urllib.parse import urljoin
#
# url = "https://ces.cdut.edu.cn/xygk/tzgg.htm"
#
# with DynamicSession(headless=False, disable_resources=False, network_idle=True) as session:
#     page = session.fetch(url, load_dom=True)
#
#     titles = page.xpath('//div[@class="list"]//ul/li/a/text()').getall()
#     links = [urljoin(url, href) for href in page.xpath('//div[@class="list"]//ul/li/a/@href').getall()]
#     dates = page.xpath('//div[@class="list"]//ul/li/span/text()').getall()
#
#     for t, d, l in zip(titles, dates, links):
#         print({"标题": t.strip(), "时间": d.strip(), "链接": l})
#
#     # 阻塞等待，避免浏览器立即退出
#     input("按回车退出浏览器...")


# from scrapling.fetchers import StealthySession
# from urllib.parse import urljoin
#
# def scrape_cdut_announcements(max_pages=3):
#     base_url = "https://ai.ahu.edu.cn/19134/list.htm"
#     current_url = base_url
#
#     with StealthySession(headless=True, solve_cloudflare=True, network_idle=True) as session:
#         for page_num in range(max_pages):
#             page = session.fetch(current_url, load_dom=True, google_search=False)
#
#             # XPath 提取标题、时间、链接
#             titles = page.xpath('//div[@class="main_conRCb"]//ul/li/a/em/text()').getall()
#             dates = page.xpath('//div[@class="main_conRCb"]//ul/li/a/span/text()').getall()
#             links = [urljoin(current_url, href) for href in page.xpath('//div[@class="main_conRCb"]//ul/li/a/@href').getall()]
#
#             # 组合成字典
#             for t, d, l in zip(titles, dates, links):
#                 print({"标题": t.strip(), "时间": d.strip(), "链接": l})
#
#             # 翻页：找到“下页”按钮的链接
#             next_page = page.xpath('//span[@class="p_next p_fun"]/a/@href').get()
#             if not next_page:
#                 break
#             current_url = urljoin(current_url, next_page)
#
# if __name__ == "__main__":
#     scrape_cdut_announcements(max_pages=3)


from scrapling.fetchers import StealthySession
from urllib.parse import urljoin

def scrape_ahu_ai_announcements(max_pages=3):
    base_url = "https://ai.ahu.edu.cn/19134/list.htm"  # 假设入口页
    current_url = base_url

    with StealthySession(headless=False, solve_cloudflare=True, network_idle=True) as session:
        for page_num in range(max_pages):
            page = session.fetch(current_url, load_dom=True)

            # 提取标题、时间、链接
            titles = page.xpath('//ul[@class="news_list list2"]/li/span[@class="news_title"]/a/text()').getall()
            links = [urljoin(current_url, href) for href in page.xpath('//ul[@class="news_list list2"]/li/span[@class="news_title"]/a/@href').getall()]
            dates = page.xpath('//ul[@class="news_list list2"]/li/span[@class="news_meta"]/text()').getall()

            # 组合成字典
            for t, d, l in zip(titles, dates, links):
                print({"标题": t.strip(), "时间": d.strip(), "链接": l})

            # 翻页：找到“下页”按钮
            next_page = page.xpath('//span[@class="p_next p_fun"]/a/@href').get()
            if not next_page:
                break
            current_url = urljoin(current_url, next_page)

if __name__ == "__main__":
    scrape_ahu_ai_announcements(max_pages=3)
