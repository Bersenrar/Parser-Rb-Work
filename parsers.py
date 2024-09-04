from playwright.sync_api import sync_playwright
from fake_useragent import UserAgent
import requests
from bs4 import BeautifulSoup
import logging
import re
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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

    @staticmethod
    def get_html_playwright(link, selector_specified=None):
        page_content = None
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(user_agent=RequestsManager.ua.random)
                page = context.new_page()
                page.goto(link)
                page.wait_for_load_state('networkidle')
                # page.wait_for_selector('div.santa-flex.santa-items-baseline.santa-space-x-10.ng-star-inserted')
                if selector_specified:
                    page.wait_for_selector(selector_specified)
                page_content = page.content()
                browser.close()
        except Exception as e:
            logger.error(f"Failed to retrieve page content: {e}")

        return page_content

    @staticmethod
    def iterate_links(links_list):
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(user_agent=RequestsManager.ua.random)
                page = context.new_page()
                for link in links_list:
                    page.goto(link)
                    page.wait_for_load_state('networkidle')
                    page.wait_for_selector("div.main-content-wrapper", timeout=50000)
                    yield page.content()
                browser.close()
        except Exception as e:
            logger.error(f"Failed to iterate over links: {e}")


class RabotaUa:
    def __init__(self):
        self.url = 'https://robota.ua/ru/candidates/{position}/{city}?{options}'
        self.candidate_url = "https://robota.ua"

    def __prepare_url(self, data):
        def process_lang():
            language_table = {
                "eng": '%5B"1"%5D', "ua": '%5B"145"%5D', "ru": '%5B"133"%5D', "pol": '%5B"130"%5D',
                "ger": '%5B"2"%5D', "slav": '%5B"136"%5D', "fre": '%5B"3"%5D'
            }
            if not data.get("language"):
                return ""
            query = "languages="
            return query + "".join(language_table.get(l, "") for l in data.get("language"))

        def process_salary():
            if not data.get("salary_from") and not data.get("salary_to"):
                return ""

            if data.get("salary_from") and data.get("salary_to"):
                query = 'salary=%7B"from"%3A{0}%2C"to"%3A{1}%7D'.format(
                    data.get("salary_from"),
                    data.get("salary_to")
                )
            elif data.get("salary_from"):
                query = 'salary=%7B"from"%3A{0}%2C'.format(
                    data.get("salary_from")
                )
            else:
                query = 'salary="to"%3A{0}%7D'.format(
                    data.get("salary_to")
                )
            return query

        def process_experience():
            experience_table = {
                "no_experience": '%5B"0"%5D', "less_1_year": '%5B"1"%5D',
                "1_3_years": '%5B"2"%2C"3"%5D', "3_5_years": '%5B"3"%5D', "5_more_years": '%5B"4"%2C"5"%5D'
            }
            if not data.get("experience"):
                return ""
            return "experienceIds=" + "".join(experience_table[e] for e in data.get("experience"))

        def process_employment():
            employment_table = {
                "full_time": 'scheduleIds=%5B"1"%5D',
                "part_time": 'scheduleIds=%5B"2"%5D',
                "both": 'scheduleIds=%5B"1"%2C"2"%5D'
            }
            if not data.get("employment"):
                return ""
            return employment_table["both"] if len(data.get("employment")) > 1 else employment_table[data.get("employment")[0]]

        def process_keywords():
            if not data.get("position"):
                return "all"
            return "-".join(data["position"])

        def process_city():
            if not data.get("city"):
                return "ukraine"
            return data.get("city").lower()

        params = [p for p in [process_employment(), process_experience(), process_salary(), process_lang()] if p]
        self.url = self.url.format(
            position=process_keywords(),
            city=process_city(),
            options="&".join(params)
        )

    def run_script(self, user_input):
        self.__prepare_url(user_input)
        result = []
        page = 1
        try:
            while page < 1000:
                current_url = self.url + f"&page={page}"
                logger.info(f"Parsing page {page}\nURL: {current_url}")
                response = RequestsManager.get_html_playwright(current_url, selector_specified="alliance-employer-cvdb-cv-list")
                resumes = self.parse_page(response)
                if resumes == "NO_CANDIDATES_LEFT":
                    logger.info("All candidates parsed")
                    break
                page += 1
                result.extend(resumes)
        except Exception as e:
            logger.info(f"Something went wrong: {e}")
        return result

    def parse_page(self, page_text):

        if not page_text:
            logger.error("Failed to load the main page content.")
            return

        soup = BeautifulSoup(page_text, "lxml")
        candidates = self.get_candidates_links(soup)
        if not candidates:
            return "NO_CANDIDATES_LEFT"
        candidates = [self.candidate_url + l for l in candidates]

        resumes = []
        k = 0
        for candidate_content in RequestsManager.iterate_links(candidates):
            candidate = self.parse_candidate(BeautifulSoup(candidate_content, "lxml"))
            candidate["link"] = candidates[k]
            print(candidate)
            resumes.append(candidate)
            k += 1
        return resumes

    @staticmethod
    def get_candidates_links(soup: BeautifulSoup):
        resumes_block = soup.find("alliance-employer-cvdb-cv-list")
        if not resumes_block:
            logger.error("Failed to find the resumes block.")
            return []

        candidates_links = []
        for resume in resumes_block.find_all("alliance-employer-cvdb-cv-list-card"):
            resume_link = resume.find("a")
            if resume_link and resume_link.get("href"):
                candidates_links.append(resume_link.get("href"))

        return candidates_links

    @staticmethod
    def parse_candidate(soup: BeautifulSoup):

        def parse_job_experience(tag: BeautifulSoup):
            user_jobs = []
            job_section = tag.find_previous("section")
            if not job_section:
                logger.error("Failed to find job section.")
                return user_jobs

            divs = job_section.find_all("div", {"class": "santa-mt-20 santa-mb-20 760:santa-mb-40 last:santa-mb-0 ng-star-inserted"})
            for div in divs:
                try:
                    job = {}
                    position = div.find("h4")
                    job_block = position.find_next_sibling("div")
                    company_name = job_block.find("div", recursive=False)
                    description = company_name.find_next_sibling("div").find("div")
                    working_time = description.find("div")
                    description = working_time.find_next_sibling("div")
                    job["position"] = position.get_text().strip() if position else None
                    job["company_name"] = company_name.get_text().strip() if company_name else None
                    job["working_time"] = working_time.get_text().strip() if working_time else None
                    job["description"] = description.get_text().strip() if description else None
                    user_jobs.append(job)
                except Exception as e:
                    logger.error(f"Something went wrong while parsing job experience: {e}")
            print(user_jobs)
            return user_jobs

        def parse_education(tag):
            user_education = []
            edu_section = tag.find_previous("section")
            if not edu_section:
                logger.error("Failed to find education section.")
                return user_education
            divs = edu_section.find_all("div")
            for div in divs:
                try:
                    edu = {}
                    institution_name = div.find("h4")
                    add_info = institution_name.find_next_sibling("div")
                    spec = add_info.find("div")
                    place_and_time = spec.find_next_sibling("div")

                    edu["name"] = institution_name.get_text() if institution_name else None
                    edu["specialisation"] = spec.get_text() if spec else None
                    edu["place_and_time"] = place_and_time.get_text() if place_and_time else None

                    user_education.append(edu)
                except Exception as e:
                    logger.error(f"Something went wrong while parsing university: {e}")

            return user_education

        def parse_skills(tag):
            skills = {"skills":[]}
            skills_block = tag.find_previous("section")
            if skills_block.find_all("br"):
                for b in skills_block.find_all("br"):
                    skills["skills"].append(b.get_text().strip())
            elif skills_block.find_all("p"):
                for b in skills_block.find_all("p"):
                    skills["skills"].append(b.get_text().strip())
            skills["full_description"] = skills_block.get_text().strip()
            # print(skills)
            return skills

        def parse_about_block(tag):

            about_block = tag.find_previous("section")
            div = about_block.find("div")
            if div:
                # print(div.get_text())
                return div.get_text().strip()

        def parse_langs(tag):
            langs_section = tag.find_previous("section")
            langs_block = langs_section.find_all("h4")
            languages = []
            for lang in langs_block:
                languages.append(lang.get_text().strip())
            # print(languages)
            return languages

        def parse_main_info():
            user_main = {}
            block = soup.find("div", {"class": "main-info-wrapper"})
            user_main["name"] = block.find("h1").get_text().strip()
            user_main["employment"] = block.find("div", {"class": "santa-mt-20"}).get_text() if block.find("div", {"class": "santa-mt-20"}) else None
            return user_main

        user_info = soup.find("article")
        if not user_info:
            logger.error("Failed to find user information section.")
            return {}

        user = {}
        all_keys = user_info.find_all("h3")

        for key_ in all_keys:
            key = key_.get_text().strip()
            if key.startswith("Прац") or key.startswith("Рабо"):
                user["job_experience"] = parse_job_experience(key_)
            elif key.startswith("Навч") or key.startswith("Учил"):
                user["education"] = parse_education(key_)
            elif key.startswith("Додаткова") or key.startswith("Дополнительная"):
                user["additional_info"] = parse_about_block(key_)
            elif key.startswith("Володіє") or key.startswith("Владеет"):
                user["languages"] = parse_langs(key_)
            elif key.startswith("Ключова") or key.startswith("Ключев"):
                user["skills"] = parse_skills(key_)
        user.update(
            parse_main_info()
        )
        return user

    def test_url(self, u_input):
        self.__prepare_url(u_input)
        print(self.url)


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
            employment_table = {"full_time": "74", "part_time": "75", "both": "74+75"}
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

