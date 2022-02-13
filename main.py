location = input("Enter a location: ").lower()
radius = int(input("Enter a radius [ONLY DIGITS]: "))


import re
import scrapy
import openpyxl
import pandas as pd
import mysql.connector
from scrapy.crawler import CrawlerProcess
from scrapy.selector import Selector


def addToDB():
 
    mydb = mysql.connector.connect(
        host = "w01b6174.kasserver.com",
        user = "d038ab61",
        password = "E2vUWAxB6shh9bEf",
        database = "d038ab61"
    )

    _cursor = mydb.cursor()
    xlsx_file = f"./dataset__stepstone__{location}.xlsx"
    wb_obj = openpyxl.load_workbook(xlsx_file) 
    sheet = wb_obj.active

    for row in sheet.iter_rows():
        rows = []
        for cell in row:
            rows.append(cell.value)
        print(f"Adding to SQL DB {rows[0]}")
        job_title = rows[0] if rows[0] else "NULL"
        company = rows[1] if rows[1] else "NULL"
        city = rows[2] if rows[2] else "NULL"
        link = rows[3] if rows[3] else "NULL"
        phone = rows[4] if rows[4] else "NULL"
        email = rows[5] if rows[5] else "NULL"
        contactdata = rows[6] if rows[6] else "NULL"

        sql = "INSERT INTO stepstone (job_title, company, city, link, email, phone, contactdata) VALUES(%s, %s, %s, %s, %s, %s, %s)"
        val = (job_title, company, city, link, phone, email, contactdata)
        _cursor.execute(sql, val)

        mydb.commit()

        print("[RECORD INSTERED]", rows[0])
    
    print("[ALL DONE]")

class StepStone(scrapy.Spider):
    name = 'stepstone'

    headers = {
        'authority': 'www.stepstone.de',
        'cache-control': 'max-age=0',
        'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="97", "Chromium";v="97"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-user': '?1',
        'sec-fetch-dest': 'document',
        'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-GB;q=0.6,en-US;q=0.5,sv;q=0.4',
        'cookie': "cfid=d2ef193e-e26e-4a6d-a1c6-feb2758dedf8; cftoken=0; USER_HASH_ID=0fe32e3a-46d2-4b7d-8c3c-394318fa970c; V5=1; UXUSER=%20%3B%20%3B%20%3B; STEPSTONEV5LANG=de; ONLINE_CF=10.147.5.29; VISITOR_ID=afd681a144ca9de1b3669e97c140f00e; s_vi=[CS]v1|ef7f3de08c24e34a-d183b4e488d5479f[CE]; s_fid=ef7f3de08c24e34a-d183b4e488d5479f; qualtrics_lp=C-empty; s_cc=true; _gcl_au=1.1.1739351604.1642852800; iom_consent=0000000000&1642852800602; CONSENTMGR=c1:1%7Cc2:1%7Cc3:1%7Cc4:1%7Cc5:0%7Cc6:1%7Cc7:1%7Cc8:0%7Cc9:1%7Cc10:0%7Cc11:0%7Cc12:1%7Cc13:1%7Cc14:0%7Cc15:0%7Cts:1642852973134%7Cconsent:true; _hjSessionUser_2060800=eyJpZCI6ImMyYTUyMWFkLWUwYmYtNTBmZi05OWMyLWQxY2E3MTBhMWM2OSIsImNyZWF0ZWQiOjE2NDI4NTI4MDE2MjQsImV4aXN0aW5nIjp0cnVlfQ==; _abck=F0D587B5A9F3B48DB6846EB2F88C6F19~0~YAAQlfAoF1zj5nl+AQAA7gjcggfdBR10Df4FG/xvAFWWvuJaF+9ImFPfsOxdLsdpps74mhkO0YO/Xqr8zi816wKrIYI5UKEkGI7RZvlzpBqw8YaJ1qlfIzOmqrJy5gpRCY0ItqcBboP4LUyV2r0sNsBBRzTwNMd7H0b0Nv7w/Pj9ezdrLXSRpBdDsZntRbGhR1Mk/aaeQXcCbea0XwX0LI7daRnGGvn1f9HnPsaRRI/Qf9VWXkqZt9gM8+GNMmEREeRwXGu5SR7j8gsrSs3LGrC8jXVxVyCHjSBh7UfdVUU3TdqJFis4O4fBWUApYihmeUmUkzmycyJ1G7pDBpkSR2nHsFmtJMvmfB737JJNkw75ukJsfelGy0f8MbIvMwMKZCFKNcBnm6xVZPJxdAzlT6PxPv9GELankzI=~-1~-1~-1; bm_sz=3FD9355D599D4DB6FABDAC5D5CD0B86A~YAAQlfAoF13j5nl+AQAA7gjcgg6ABzdavK8RmjqB8+t1+ZOsp8ImMs6f6pP8XtUXZMXA/+SPi/dyydlR9+4DbkItM7ldGQz4IVUVnAKjX1hJayPL5DZNIL5JPIcu9VlUyWYVRq9ncopncmzTUilOayDbuJ80MsarVhBiuqAaB6Lo86pis/9aAPg0YQCJ2lPpgEWHtCP00TBRTHKNBIdE5DGRHULOFuEh57ggjnGALSFAzqcyhDf2E6zEO+EzvQQ8E4pq3LSxpblhXNSSVReMo+BvpumdVcMQ4d/yOTf3D5GImDHeuw==~3687473~4536121; g_state={\"i_p\":1642880312362,\"i_l\":1}; APPVER=v5; X-AUTH-CSRF-TOKEN=hxc9khyf876866qcnpp4bnqi7caw6l3b47bgv0qd; JSESSIONID=4D4C09353229985C4982AA0384415F08; _hjSession_2060800=eyJpZCI6Ijc5ODkyOTAwLTQ4NGMtNDA4Yi1hMTIzLTc5ODE2YzliMGU0YiIsImNyZWF0ZWQiOjE2NDI4ODQ4NDE3MDcsImluU2FtcGxlIjpmYWxzZX0=; _hjIncludedInSessionSample=0; _hjAbsoluteSessionInProgress=0; _uetsid=d346e7307b7a11ec9ac58d5a9949a481; _uetvid=d34738307b7a11ec9906332c30deb795; cto_bundle=mrPj0V9halhabE9iU0FsJTJGMXF2SW9mZGhKMXlOaHk5WXpIJTJGc01zYXVROVdQNDdMZ00wc2JOdnhkYUNqeEd4eUJKdlFKYVF4VmVIZUw0QXJTR1A0WG8lMkZXTGFoUzVxOW5PaVVTUktmQkVSOEU5cEoyekxUWHlybEJxcGxIUUl4d0lBSEFyTDVpQnBXU2tQcTNiSW9wc0FNMjBoWlElM0QlM0Q; utag_main=v_id:017e81a7fd640022e886b989405a05072004506a00bd0{_sn:3$_se:20$_ss:0$_st:1642887086550$dc_visit:3$vapi_domain:stepstone.de$vpn:ext$ses_id:1642884840186%3Bexp-session$_pn:4%3Bexp-session$dc_event:12%3Bexp-session$vpntest:b%3Bexp-session$prev_p:Resultlist%20Responsive%3Bexp-session$dc_region:eu-central-1%3Bexp-session;} s_sq=%5B%5BB%5D%5D; ioam2018=001ec016602d9f61e61ebf1bf:1669118400604:1642852800604:.stepstone.de:17:stepston:core_search:noevent:1642885286563:q665oc; _dd_s=rum=1&id=3a3751bd-9e01-4d32-bedb-1fba2d0d8b70&created=1642884838444&expire=1642886707254; RT=\"z=1&dm=stepstone.de&si=0501b660-1692-4b34-b88b-6271e2997e0e&ss=kyqb7nqe&sl=6&tt=drh&bcn=%2F%2F684d0d47.akstat.io%2F&rl=1&nu=10qanjb8e&cl=9n2v&obo=1&ld=kx0e&r=10qanjb8e&ul=kx0f\"",
    }
    
    custom_settings = {
        "FEED_FORMAT": 'csv',
        "FEED_URI": f'dataset__stepstone__{location}.csv'
    }

    def start_requests(self):
        url = f'https://www.stepstone.de/5/ergebnisliste.html?where={location}&radius={radius}'
        yield scrapy.Request(url=url, headers=self.headers, callback=self.parse)

    def parse(self, response):
        links = response.xpath('//a[@data-at="job-item-title"]')
        for each in links:
            link = each.xpath('.//@href').get()
            
            yield response.follow(url=link, headers=self.headers, callback=self.parse_links)
        
        next_page = response.xpath('//a[@data-at="pagination-next"]/@href').get()
        if next_page:
            yield response.follow(url=next_page, headers=self.headers, callback=self.parse)
            
    def parse_links(self, response):
        job_title = response.xpath('normalize-space(//h1[contains(@class, "jobTitle")]/text())').get()
        company_name = response.xpath('normalize-space(//a[contains(@class, "company-name")]/text())').get()
        location = response.xpath('normalize-space(//li[contains(@class, "icons_location")]//text())').get()
        link_to_job = response.url
        email_selector = response.xpath('//div[contains(@class, "sc-jtRfpW")]').get()
        try:
            email = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', email_selector)[0]
        except:
            email = ''
        phone = response.xpath('normalize-space(//a[contains(@href, "tel:")]/text())').get()
        contact_data = response.xpath('normalize-space(//div[contains(@class, "sc-jtRfpW")])').get()
        listing_id_selector = response.xpath('//div[contains(@data-replyone, "listingId")]/@data-replyone').get()
        job_id = re.findall(r'"listingId":(.*?),', listing_id_selector)
        
        print(job_title)

        yield {
            "Job Title": job_title,
            "Company": company_name,
            "City": location,
            "Link to Job": link_to_job,
            "Email": email,
            "Phone": phone,
            "Contact Data": contact_data,
            "Job ID": ", ".join(job_id),
        }


if __name__ == '__main__':
    process = CrawlerProcess()
    process.crawl(StepStone)
    process.start()
