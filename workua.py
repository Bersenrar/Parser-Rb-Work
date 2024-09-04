import threading

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import logging
import pandas as pd
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



# https://www.work.ua/resumes-lviv-%D0%BF%D1%80%D0%BE%D0%B3%D1%80%D0%B0%D0%BC%D0%B8%D1%81%D1%82+%D1%82%D0%B5%D1%80+22412/?anyword=1&notitle=1

work_ua_url = "https://www.work.ua/resumes" \
               "{city}-{position}/?" \
               "notitle=1&anyword=1&" \
               "{employment}&{experience}&{salary}&{language}"


class RequestsManager:
    ua = UserAgent()

    @staticmethod
    def make_request(url, method="GET"):
        try:
            proxies = {"https": "socks5://127.0.0.1:9050"}
            response = requests.request(method=method, url=url, headers={"user-agent":RequestsManager.ua.random}, proxies={})
            if response.status_code != 200:
                return None
            response.encoding = response.apparent_encoding
            return response.text
        except requests.RequestException as e:
            logger.error(f"Something went wrong while requesting to {url}\nError: {e}")


class WorkuaParser:

    def __init__(self):
        self.base_url = "https://www.work.ua/resumes"
        self.url = ""
        self.resume_url_base = "https://www.work.ua"
        self.config = {}

    def __prepare_url(self, data):
        def process_lang():
            langs_table = {
                "eng": "1", "ua": "41", "ru": "32", "pol": "3",
                "ger": "2", "slav": "34", "fre": "5"
            }
            if not data.get("language"):
                return ""
            return "language=" + '+'.join(langs_table.get(l, "") for l in data["language"])

        def process_employment():
            employment_table = {"full_time": "74", "part_time": "75"}
            if not data.get("employment"):
                return ""
            return "employment=" + '+'.join(employment_table.get(e, "") for e in data["employment"])

        def process_salary():
            def get_closest_salary(salary, salary_table):
                salaries = map(int, salary_table.keys())
                return salary_table[str(min(salaries, key=lambda x: abs(x - salary)))]

            salary_table = {
                "10000": "2", "15000": "3", "20000": "4",
                "30000": "5", "40000": "6", "50000": "7", "100000": "8"
            }
            if not data.get("salary_from") and not data.get("salary_to"):
                return ""

            query = []
            if data.get("salary_from"):
                query.append(f"salaryfrom={get_closest_salary(int(data['salary_from']), salary_table)}")
            if data.get("salary_to"):
                query.append(f"salaryto={get_closest_salary(int(data['salary_to']), salary_table)}")
            return "&".join(query)

        def process_experience():
            experience_table = {
                "no_experience": "0", "less_1_year": "1",
                "1_3_years": "164", "3_5_years": "165", "5_more_years": "166"
            }
            if not data.get("experience"):
                return ""
            return "experience=" + '+'.join(experience_table.get(e, "") for e in data["experience"])

        def process_keywords():
            if not data.get("position"):
                return "Всі"
            return "+".join(data["position"])

        def process_city():
            return "-" + data["city"].lower() if data.get("city") else ""

        params = [p for p in [process_employment(), process_experience(), process_salary(), process_lang()] if p]
        self.url = f"{self.base_url}{process_city()}-{process_keywords()}/?notitle=1&anyword=1&{'&'.join(params)}"

    @staticmethod
    def __parse_resume_links(resume_block):
        resumes_links = []
        for resume in resume_block.find_all("div", {"class": "card card-hover card-search resume-link card-visited wordwrap"}):
            resumes_links.append(resume.find("h2", {"class": "mt-0"}).find("a").get("href"))
        return resumes_links

    def __parse_page(self, soup):
        resume_block = self.__get_resume_list(soup)
        if not resume_block:
            return []

        candidates_links = self.__parse_resume_links(resume_block)
        threads = []
        result = []
        lock = threading.Lock()

        def worker(link):
            r = WorkuaParser.__parse_candidate(link)
            with lock:
                result.append(r)

        for l in candidates_links:
            url = self.resume_url_base + l
            thread = threading.Thread(
                target=worker,
                args=(url,)
            )
            threads.append(thread)
            thread.start()

        for t in threads:
            t.join()
        return result

    @staticmethod
    def __parse_candidate(link):
        text_content = RequestsManager.make_request(link)
        soup = BeautifulSoup(text_content, parser="lxml")
        check_file_resume = [p.get_text().strip() == "Завантажений файл" for p in
                             soup.find_all("h2", {"class": "mb-0"})]

        user_data = {"link": link}
        if True in check_file_resume:
            user_data["resume_type"] = "FILE"
            content_block = soup.find("div", {"class": "wordwrap", "id": "add_info"})
            WorkuaParser.plain_text_parser(content_block, user_data)
        else:
            user_data["resume_type"] = "WORKUA"
            resume_id = link.split("/")[-2]
            content_block = soup.find("div", {"id": f"resume_{resume_id}"})
            WorkuaParser.work_ua_template_parser(content_block, user_data)

        return user_data

    @staticmethod
    def plain_text_parser(profile, user):
        profile = profile.get_text()
        name_pattern = re.compile(r'^[A-ZА-ЯЇІЄҐ][a-zа-яїієґ]+\s[A-ZА-ЯЇІЄҐ][a-zа-яїієґ]+', re.MULTILINE)
        education_pattern = re.compile(r'(?:EDUCATION|Освіта)(.*?)\n(?:\n|$)', re.DOTALL | re.IGNORECASE)
        skills_pattern = re.compile(r'(?:SKILLS|Навички|TECH SKILLS)(.*?)\n(?:\n|$)', re.DOTALL | re.IGNORECASE)
        experience_pattern = re.compile(r'(?:EXPERIENCE|Досвід роботи|WORK EXPERIENCE)(.*?)\n(?:\n|$)',
                                        re.DOTALL | re.IGNORECASE)

        name_match = name_pattern.search(profile)
        name = name_match.group(0).strip() if name_match else "Not found"

        education_match = education_pattern.search(profile)
        education = education_match.group(1).strip() if education_match else "Not found"

        skills_match = skills_pattern.search(profile)
        skills = skills_match.group(1).strip() if skills_match else "Not found"

        experience_match = experience_pattern.search(profile)
        experience = experience_match.group(1).strip() if experience_match else "Not found"
        user["name"] = name
        user["job_experience"] = experience
        user["education"] = education
        user["skills_stack"] = skills
        return user

    @staticmethod
    def work_ua_template_parser(soup, user):
        def parse_base_info(base_info, user):
            try:
                user["name"] = base_info.find("h1").get_text().strip()
                spec = base_info.find("h2").get_text().strip()
                if "грн" in spec:
                    spec = spec.split(",")
                    salary = spec[-1]
                    spec = " ".join(spec[:-1])
                    user["salary"] = salary
                user["occupation"] = spec

                # Дополнительная информация
                dt, dd = base_info.find_all("dt"), base_info.find_all("dd")
                for k, v in zip(dt, dd):
                    k, v = k.get_text().strip(), v.get_text().strip()
                    if "Вік" in k:
                        user["age"] = v
                    elif "Зайнятість" in k:
                        user["employment_type"] = v
                    elif "Місто проживання" in k:
                        user["living_city"] = v
                    elif "Готовий працювати" in k:
                        user["working_where"] = v
                    else:
                        user["unprocessed_info"].append((k, v))
            except AttributeError as e:
                logger.error(f"Something went wrong while parsing base info: {e}")
                user["unprocessed_info"].append("Base info wasn't parsed")

        def parse_job_experience(soup):
            job_experience = []
            try:
                job_tag = None
                for h_2 in soup.find_all("h2"):
                    if h_2 and "Досвід" in h_2.get_text(strip=True).strip():
                        job_tag = h_2
                        break
                if job_tag:
                    job_tag = job_tag.find_next_sibling("h2")
                    while job_tag and "Осв" not in job_tag.get_text().strip():
                        job_title = job_tag.get_text().strip()

                        next_p = job_tag.find_next_sibling("p")
                        company_name_working_time = next_p.get_text().strip() if next_p else "No company name provided"

                        next_p = job_tag.find_next_sibling("p")
                        description_p = next_p.find_next_sibling("p") if next_p else None
                        description = description_p.get_text(
                            strip=True).strip() if description_p else "No description provided"

                        pos = {"title": job_title, "company_name_and_time": company_name_working_time,
                               "description": description}
                        job_experience.append(pos)
                        job_tag = job_tag.find_next_sibling("h2")

                return job_experience, job_tag
            except AttributeError as e:
                logger.error(f"Error occured while parsing working experience: {e}")
                return job_experience

        def parse_education(job_tag):
            """Парсинг образования."""
            education = []
            try:
                education_tag = job_tag.find_next_sibling("h2") if job_tag else None

                if education_tag:
                    while education_tag and "Знання" not in education_tag.get_text().strip():
                        uni = {}
                        university = education_tag.get_text().strip()
                        description = education_tag.find_next_sibling("p").get_text(strip=True).strip()
                        uni["title"] = university
                        uni["description"] = description
                        education.append(uni)
                        education_tag = education_tag.find_next_sibling("h2")
            except AttributeError as e:
                logger.error(f"Education info wasnt parsed: {e}")
            finally:
                return education

        def parse_skills(skills_tag):
            """Парсинг навыков."""
            skill_list = []
            try:
                if skills_tag:
                    for li in skills_tag.find_next_sibling("ul").find_all("li"):
                        skill_list.append(li.find("span").get_text().strip())
                return skill_list
            except AttributeError as e:
                logger.error(f"Skills info wasnt parsed: {e}")
                return skill_list

        def parse_languages(skills_tag):
            """Парсинг языков."""
            language_list = []
            try:
                if skills_tag and skills_tag.find_next_sibling("h2"):
                    language_tag = skills_tag.find_next_sibling("h2")
                    for li in language_tag.find_next_sibling("ul").find_all("li"):
                        language_list.append(li.get_text().strip())
                return language_list
            except AttributeError as e:
                logger.error(f"languages info wasnt parsed: {e}")
                return language_list

        user["unprocessed_info"] = []
        base_info = soup.find("div", {"class": "mt-lg"})

        parse_base_info(base_info, user)

        user["job_experience"], job_tag = parse_job_experience(soup)

        user["education"] = parse_education(job_tag)

        skills_tag = soup.find("h2", {"class": "mb-sm"})
        user["skill_stack"] = parse_skills(skills_tag)

        user["language"] = parse_languages(skills_tag)

        return user

    @staticmethod
    def __get_resume_list(soup):
        return soup.find("div", {"id": "pjax-resume-list"})

    @staticmethod
    def __count_pages(soup):
        pages = 0
        for nav in soup.find_all("nav"):
            for li in nav.find_all("li"):
                span = li.find("span")
                if span:
                    span_title = span.get("title")
                    if span_title and span_title.startswith("Стор."):
                        pages = int(span_title.split()[-1])
        return pages

    def run_script(self, user_input):
        self.__prepare_url(user_input)
        logger.info(f"Created url: {self.url}")
        page_text = RequestsManager.make_request(self.url)

        if not page_text:
            logger.info("Page doesn't received")
            return

        soup_obj = BeautifulSoup(page_text, parser="lxml")
        pages_amt = self.__count_pages(soup_obj)

        if pages_amt == 0:
            logger.info("No pages found")

        resumes = []
        try:
            resumes.extend(self.__parse_page(soup_obj))
            for i in range(2, pages_amt):
                page_text = RequestsManager.make_request(self.url + f"&page={i}")
                soup_obj = BeautifulSoup(page_text, parser="lxml")
                resumes.extend(self.__parse_page(soup_obj))
        except Exception as e:
            logger.error(f"Something went wrong: {e}")
        return resumes


if __name__ == "__main__":
    wp = WorkuaParser()
    # u_input = {
    #     "position": ["Программист", "Python", "Java"],
    #     "city": "Dnipro",
    #     "salary_from": 8000,
    #     "salary_to": 14000,
    #     "language": ["eng", "ua"],
    #     "experience": ["no_experience"],
    #     "employment": ["part_time"]
    # }
    u_input = {
        "position": ["Python", "JAVA"],
        "city": "remote"
    }
    wp.run_script(u_input)

