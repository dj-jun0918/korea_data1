from argparse import ArgumentParser, Namespace
from copy import deepcopy
from multiprocessing.pool import Pool
from time import sleep
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import requests
import ujson as json
from bs4 import BeautifulSoup
from pandas import date_range
from trafilatura import extract, fetch_url
from trafilatura.settings import DEFAULT_CONFIG

argparser = ArgumentParser("Crawl Naver news articles")
argparser.add_argument("--output-path", type=str, default="news.json")
argparser.add_argument("--query", type=str, nargs="+", default="반도체") # nargs를 사용해 한개 값 이상 받기
argparser.add_argument("--start-date", type=str, default="2024.07.15")
argparser.add_argument("--end-date", type=str, default="2024.08.15")
argparser.add_argument("--num-workers", type=int, default=10)

def news_body(url:str) -> Optional[Dict[str, Any]]: # 뉴스 본문 가져오기
    try:
        dowloaded = fetch_url(url, config=DEFAULT_CONFIG)
        collect_new_content = extract(
            dowloaded, 
            output_format="json", 
            include_tables=False, 
            with_metadata=True,
            deduplicate=True, 
            config=DEFAULT_CONFIG,
        )
        collect_new_content = json.loads(collect_new_content)
    except KeyboardInterrupt: #컨트롤 c를 눌러서 강제로 종료
        exit()
    except:  # 특정 기사에서 막혀도 다음 기사로 넘어갈 수 있게 예외 처리
        return None

    return collect_new_content

def crawl_news(args: Namespace) -> List[Dict[str, str]]:  #기사 크롤링 함수
    dates = date_range(args.start_date, args.end_date, freq="D")
    encoded_query = quote(" OR ".join(args.query))  # args.query가 리스트 이므로 검색어를 or로 연결

    crawled_urls = set()  # url 중복 수집 x
    crawled_articles = []  #뉴스들 저장할 배열

    for date in dates:
        date_str = date.strftime("%Y%m%d")

        next_url = (
            "https://s.search.naver.com/p/newssearch/search.naver?"
            f"query={encoded_query}&sort=0related=0&"
            f"nso=so%3Ar%2Cp%3Afrom{date_str}to{date_str},a:all&where=news_tab_api"
        )
        num_trials = 0
        while next_url != "":  #next_url이 빈 문자열 일 때까지 반복
            try:
                request_result = requests.get(next_url)  #next_url로 지정된 페이지에 http 요청 보내고 응답 받기
                request_result = request_result.json()  #json으로 변환
            except KeyboardInterrupt: # 예외처리
                exit()
            except Exception as e: #오류 발생시 5초 대기후 다시 실행                
                sleep(5)
                num_trials += 1
                if num_trials == args.max_trials:
                    break
                else:
                    continue

            contents = request_result["contents"]
            next_url = request_result["nextUrl"]

            article_urls = []
            for content in contents: #링크 추출
                content_soup = BeautifulSoup(content, features="lxml")  
                news = content_soup.find("a", {"class": "news_tit"})
                news_url = news["href"]

                if news_url not in crawled_urls:  # 새로운 url만 추가함
                    article_urls.append(news_url)

            with Pool(args.num_workers) as pool: # news_body를 병렬적으로 실행하기
                for article_body in pool.imap_unordered(news_body, article_urls): #url 가져와서 함수를 호출해 본문 수집
                    if article_body is not None: #리스트 안에 없을 때 
                        crawled_articles.append(article_body)  # 리스트에 추가
            crawled_urls.update(article_urls)  # article_url에 있는 url들 추가
    return crawled_articles

if __name__ == "__main__":
    args = argparser.parse_args()

    crawled_articles = crawl_news(args)

    with open(args.output_path, "w", encoding="utf-8") as f:
        json.dump(crawled_articles, f, ensure_ascii=False)