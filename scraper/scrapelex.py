from bs4 import BeautifulSoup
import requests
import json
from time import sleep
from datetime import datetime
from re import sub
import logging
from os import makedirs, path, listdir
import gzip
from tqdm import tqdm
import languagecodes
from multiprocessing import Pool
from pagerange import PageRange


class EURlexScraper:
    def __init__(self, lang="it", log_level=0):
        """
        Initialize the EurLexScraper object

        :param lang: language of the EUR-lex website.
        :param log_level: logging level. Available values: 0, 1, 2.
        """
        if log_level not in {0, 1, 2, 3}:
            raise ValueError("Invalid log level. Available values: 0, 1, 2.")

        if log_level == 0:
            log_level = logging.ERROR
        elif log_level == 1:
            log_level = logging.WARNING
        elif log_level == 2:
            log_level = logging.INFO

        logging.basicConfig(
            level=log_level,
            format="%(asctime)s - %(message)s",
            datefmt="%d-%b-%y %H:%M:%S",
        )

        # Disable urllib3 logging
        logging.getLogger("urllib3").setLevel(logging.ERROR)

        # Set obtained from EUR-lex website
        self.lang_set = {
            "bg",
            "es",
            "cs",
            "da",
            "de",
            "et",
            "el",
            "en",
            "fr",
            "ga",
            "hr",
            "it",
            "lv",
            "lt",
            "hu",
            "mt",
            "nl",
            "pl",
            "pt",
            "ro",
            "sk",
            "sl",
            "fi",
            "sv",
        }
        self.__validate_languages(lang)
        self.lang = lang

        alpha3 = languagecodes.iso_639_alpha3(self.lang).strip().upper()
        self.base_url = f"https://eur-lex.europa.eu/search.html?SUBDOM_INIT=ALL_ALL&DTS_SUBDOM=ALL_ALL&DTS_DOM=ALL&lang={self.lang}&locale={self.lang}&type=advanced&wh0=andCOMPOSE%3D{alpha3}%2CorEMBEDDED_MANIFESTATION-TYPE%3Dpdf%3BEMBEDDED_MANIFESTATION-TYPE%3Dpdfa1a%3BEMBEDDED_MANIFESTATION-TYPE%3Dpdfa1b%3BEMBEDDED_MANIFESTATION-TYPE%3Dpdfa2a%3BEMBEDDED_MANIFESTATION-TYPE%3Dpdfx%3BEMBEDDED_MANIFESTATION-TYPE%3Dpdf1x%3BEMBEDDED_MANIFESTATION-TYPE%3Dhtml%3BEMBEDDED_MANIFESTATION-TYPE%3Dxhtml%3BEMBEDDED_MANIFESTATION-TYPE%3Ddoc%3BEMBEDDED_MANIFESTATION-TYPE%3Ddocx"
        self.base_url_year = f"https://eur-lex.europa.eu/search.html?SUBDOM_INIT=ALL_ALL&DTS_SUBDOM=ALL_ALL&DTS_DOM=ALL&lang={self.lang}&locale={self.lang}&type=advanced&wh0=andCOMPOSE%3D{alpha3}%2CorEMBEDDED_MANIFESTATION-TYPE%3Dpdf%3BEMBEDDED_MANIFESTATION-TYPE%3Dpdfa1a%3BEMBEDDED_MANIFESTATION-TYPE%3Dpdfa1b%3BEMBEDDED_MANIFESTATION-TYPE%3Dpdfa2a%3BEMBEDDED_MANIFESTATION-TYPE%3Dpdfx%3BEMBEDDED_MANIFESTATION-TYPE%3Dpdf1x%3BEMBEDDED_MANIFESTATION-TYPE%3Dhtml%3BEMBEDDED_MANIFESTATION-TYPE%3Dxhtml%3BEMBEDDED_MANIFESTATION-TYPE%3Ddoc%3BEMBEDDED_MANIFESTATION-TYPE%3Ddocx"
        self.r = requests.Session()
        self.r.headers.update(
            {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": f"{self.lang},en-US;q=0.7,en;q=0.3",
                "Connection": "keep-alive",
                "DNT": "1",
                "Host": "eur-lex.europa.eu",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "cross-site",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/116.0",
            }
        )

        self.year_list = []

        for year in range(datetime.now().year - 1, 1800, -1):
            self.year_list.append(str(year))

        self.year_list.append("1001")
        self.year_list.append("?")

        self.cooldowns = 0

        self.document_types = {}

        # Types of documents available on EUR-lex in the advanced search form.
        with open(
            path.join(path.dirname(path.realpath(__file__)), "searchTypes.txt"), "r"
        ) as fp:
            for line in fp:
                splitted_line = line.split("(")
                self.document_types[
                    splitted_line[-1].replace(")", "").strip()
                ] = "(".join(splitted_line[:-1]).strip()
            del splitted_line

        with open(
            path.join(path.dirname(path.realpath(__file__)), "label_mapping.json"),
            "r",
            encoding="utf-8",
        ) as file:
            self.label_mappings = json.load(file)

    def __validate_languages(self, lang):
        """
        Validate the languages in the languages set
        """
        if lang not in self.lang_set:
            raise ValueError(f"Invalid language: {lang}")

    def __set_cookies(self):
        """
        Set the cookies for the session
        """
        keep_trying = True
        while keep_trying:
            try:
                res = self.r.get(
                    f"https://eur-lex.europa.eu/search.html?scope=EURLEX&lang={self.lang}&type=quick&qid={int(datetime.now().timestamp())}",
                    timeout=60,
                )
                keep_trying = False
            except:
                logging.warning("Error setting cookies. Retrying...")
                sleep(10)
        if res.ok:
            self.r.cookies.update(res.cookies)
        else:
            raise ("Error setting cookies. Check your internet connection.")

    def __reset_session(self):
        """
        Utility function to reset the session
        """
        logging.warning("Resetting session...")
        self.r = requests.Session()
        self.r.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:108.0) Gecko/20100101 Firefox/108.0",
                "Accept-Language": f"{self.lang},en-US;q=0.7,en;q=0.3",
            }
        )
        self.__set_cookies()

    def __clean_text(self, text):
        """
        Utility function to clean the text

        :param text: text to clean
        :return: cleaned text
        """
        return text.replace("\xa0", " ").replace("’", "'").replace("´", "'")

    def __scrape_page(self, page_html, label_types):
        """
        Utility function to scrape the needed information from the page

        :param page_html: html of the page
        :param label_types: label types to scrape.
        :return: list of eurovoc classifiers and full text of the document
        """
        page_html = sub(r"<br[/ ]*>", "\n", page_html)
        soup = BeautifulSoup(page_html, "lxml")
        eurovoc_classifiers = []
        full_text = ""
        label_types = label_types.split(",")
        if any(label_type not in {"TC", "MT", "DO"} for label_type in label_types):
            raise ValueError("Invalid label type. Accepted values: TC, MT, DO.")

        page_classifiers = soup.find("div", {"id": "PPClass_Contents"})
        if page_classifiers and page_classifiers.find("ul"):
            eurovoc_classifiers = [
                classifier.find("a")["href"].split("DC_CODED=")[1].split("&")[0].strip()
                for classifier in page_classifiers.find("ul").find_all("li")
                if classifier.find("a") and "DC_CODED=" in classifier.find("a")["href"]
            ]

        tc, mt, do = set(), set(), set()
        for label_type in label_types:
            if label_type == "TC":
                tc = set(eurovoc_classifiers)
            elif label_type == "MT":
                for classifier in eurovoc_classifiers:
                    if classifier in self.label_mappings:
                        mt.add(self.label_mappings[classifier] + "_mt")
            elif label_type == "DO":
                for classifier in eurovoc_classifiers:
                    if classifier in self.label_mappings:
                        do.add(self.label_mappings[classifier][:2] + "_do")

        eurovoc_classifiers = list(tc.union(mt).union(do))

        text_element = None
        consolidated = False
        if soup.find("div", id="TexteOnly"):
            text_element = soup.find("div", id="TexteOnly").find("txt_te")
        elif soup.find("p", {"class": "oj-doc-ti"}) or soup.find(
            "p", {"class": "doc-ti"}
        ):
            text_element = (
                soup.find("div", {"id": "document1"})
                .find("div", {"class": "tabContent"})
                .find("div")
            )
        elif soup.find("p", {"class": "disclaimer"}):
            text_element = (
                soup.find("div", {"id": "document1"})
                .find("div", {"class": "tabContent"})
                .find("div")
            )
            consolidated = True

        if text_element:
            """if texteonly:
            full_text += (
                self.__clean_text(
                    soup.find("div", {"id": "document1"})
                    .find("div", {"class": "tabContent"})
                    .find("strong")
                    .text
                )
                + " "
            )"""

            skip = True
            for child in text_element.children:
                if child.name == "p":
                    if consolidated:
                        if child.has_attr("class"):
                            if (
                                "reference" in child["class"]
                                or "disclaimer" in child["class"]
                                or "hd-modifiers" in child["class"]
                                or "arrow" in child["class"]
                            ):
                                continue
                            if "title-doc-first" in child["class"]:
                                skip = False
                    if child.has_attr("class"):
                        if "footnote" in child["class"] or "modref" in child["class"]:
                            continue
                    full_text += self.__clean_text(child.text) + "\n"
                elif child.name == "div":
                    if consolidated and skip:
                        continue
                    for p in child.find_all("p"):
                        full_text += self.__clean_text(p.text) + "\n"
                elif child.name == "table":
                    if consolidated and skip:
                        continue
                    for tr in child.find_all("tr"):
                        full_text += self.__clean_text(tr.text) + "\n"
                elif child.name == "hr":
                    if consolidated and skip:
                        continue
                    full_text += "[SEP]"

        # full_text = full_text.replace("\n", " ")
        full_text = full_text.replace("◄", "")
        full_text = (
            full_text.split("[SEP]", maxsplit=1)[1].replace("[SEP]", "")
            if len(full_text) > 0 and "[SEP]" in full_text
            else full_text
        )
        full_text = sub("►\D\d+", "", full_text)
        full_text = sub(" +", " ", full_text).strip()
        full_text = sub("\n+", "\n", full_text).strip()

        return eurovoc_classifiers, full_text

    def __get_full_document(
        self,
        endpoint,
        max_retries=10,
        log_errors=True,
        directory="./",
        scrape=True,
        label_types="TC",
    ):
        """
        Extract information from an individual document page from EUR-lex

        :param endpoint: page endpoint
        :param max_retries: max number of retries.
        :param log_errors: log errors to a file.
        :param directory: directory of the error file.
        :param scrape: scrape the page.
        :param label_types: label types to scrape.
        :return: list of eurovoc classifiers and full text of the document
        """
        keep_trying = True
        count = 0
        eurovoc_classifiers = []
        full_text = ""
        page_html = ""
        while keep_trying and count < max_retries:
            try:
                self.r = requests.get(endpoint, timeout=120)
            except:
                logging.error(f"Error fetching page {endpoint}, trying again")
                count += 1
                if count > 2:
                    logging.warning("Cooldown...")
                    self.cooldowns += 1
                    if self.cooldowns > 5:
                        self.__reset_session()
                        self.cooldowns = 0
                    sleep(60)
                else:
                    sleep(1)
                continue
            if self.r.ok:
                keep_trying = False
                page_html = self.r.text
                if scrape:
                    eurovoc_classifiers, full_text = self.__scrape_page(
                        page_html, label_types
                    )

            else:
                if self.r.status_code == 404:
                    logging.warning(f"Page {endpoint} not found")
                    with open(
                        path.realpath(path.join(directory, "not_found.txt")),
                        "a",
                        encoding="utf-8",
                    ) as fp:
                        fp.write(endpoint + "\n")
                    break

                logging.error(
                    f"Error fetching page {endpoint}. Status code: {self.r.status_code}, trying again"
                )
                count += 1
                if count > 2:
                    logging.warning("Cooldown...")
                    self.cooldowns += 1
                    if self.cooldowns > 5:
                        self.__reset_session()
                        self.cooldowns = 0
                    sleep(60)
                else:
                    sleep(count + 1)

        if count >= max_retries:
            logging.error(f"Max retries reached for page {endpoint}")
            if log_errors:
                with open(
                    path.realpath(path.join(directory, "errors.txt")),
                    "a",
                    encoding="utf-8",
                ) as fp:
                    fp.write(endpoint + f" unreachable at {datetime.now()}\n")

        return page_html, eurovoc_classifiers, full_text

    def __get_documents_info(self, soup):
        """
        Retrieve info of all the results of a search page

        :param soup: page content
        :return: dictionary of information for each document
        """
        to_return = {}
        page_results = soup.find_all("div", {"class": "SearchResult"})
        for result in page_results:
            if result.find("h2").find("a", class_="not-linkable-portion"):
                continue
            title = self.__clean_text(result.find("h2").find("a").text.strip())
            doc_id = (
                result.find("h2")
                .find("a", class_="title")["href"]
                .split("uri=")[1]
                .split("&")[0]
                .replace("/", "-")
                .replace(":", "-")
            )

            link = result.find("h2").find("a")["name"]
            to_return[doc_id] = {
                "title": title,
                "link": link,
            }
        return to_return

    def __save_file(self, directory, content):
        """
        Utility function to save a gzipped HTML file

        :param directory: directory of the file
        :param content: content of the file
        """
        with gzip.open(f"{directory}.gz", "wb") as fp:
            fp.write(content)

    def __save_checkpoint(self, directory, search_endpoint, doc_endpoint):
        """
        Utility function to save a checkpoint file

        :param directory: directory of the file
        :param search_endpoint: last search endpoint
        :param doc_endpoint: last document endpoint
        """
        with open(directory, "w", encoding="utf-8") as fp:
            json.dump(
                {
                    "last_search_endpoint": search_endpoint,
                    "last_doc_endpoint": doc_endpoint,
                },
                fp,
                ensure_ascii=False,
                indent=4,
            )

    def __get_documents_search(
        self,
        search_term,
        terms=[],
        log_errors=True,
        save_html=False,
        save_data=False,
        directory="./",
        max_retries=10,
        sleep_time=0,
        n=0,
        mode="year",
        resume_params=None,
        skip_existing=True,
        label_types="TC",
    ):
        """
        General function that scrapes documents from the search page
        NOTE: the 'n' parameter is not implemented yet

        :param search_term: search term to use
        :param terms: list of terms to scrape.
        :param log_errors: whether to log errors in a file.
        :param save_html: whether to save the html of each document.
        :param save_data: whether to save the scraped data of each category in its own file. Pass 'False' if you want to handle the saving yourself.
        :param directory: directory to save the scraped data.
        :param max_retries: maximum number of retries for each page, both search pages and individual documents.
        :param sleep_time: time to sleep between each document request.
        :param n: number of documents to scrape.
        :param mode: whether to scrape by year or category.
        :param resume_params: dictionary containing the parameters to resume scraping.
        :param skip_existing: whether to skip documents that have already been scraped.
        :param label_types: label types to scrape.
        :return: dictionary of documents
        """
        documents = {}
        page = 1

        if mode == "year":
            base_url = self.base_url_year
        elif mode == "category":
            base_url = self.base_url

        for term in terms:
            logging.info(f"Scraping {mode} {term}...")
            if resume_params:
                page = int(resume_params["page"])
                if term != resume_params["term"]:
                    continue
                else:
                    resume_params = None

            dirterm = term if term != "?" else "unknown"
            term = term if term != "?" else "FV_OTHER"
            makedirs(f"{directory}/searchHTML/{dirterm}", exist_ok=True)
            makedirs(f"{directory}/docsHTML/{dirterm}", exist_ok=True)
            end = False
            count = 0
            total_pages = 0
            while not end:
                endpoint = (
                    base_url
                    + search_term
                    + term
                    + f"&qid={int(datetime.now().timestamp())}"
                    + f"&page={page}"
                )
                try:
                    self.r = requests.get(endpoint, timeout=60)
                except:
                    logging.error(f"Connection error for {endpoint}, cooling down...")
                    sleep(60)
                    continue
                if self.r.ok:
                    count = 0
                    if save_html:
                        self.__save_file(
                            f"{directory}/searchHTML/{dirterm}/{page}.html",
                            self.r.content,
                        )

                    soup = BeautifulSoup(self.r.text, "lxml")
                    if term not in documents:
                        documents[term] = {}
                    documents_in_page = self.__get_documents_info(soup)

                    skip_count = 0
                    for doc_id, doc_info in documents_in_page.items():
                        documents[term][doc_id] = doc_info
                        documents[term][doc_id]["eurovoc_classifiers"] = []
                        documents[term][doc_id]["full_text"] = ""

                        if skip_existing and path.exists(
                            f"{directory}/docsHTML/{dirterm}/{doc_id}.html.gz"
                        ):
                            skip_count += 1
                            continue

                        (
                            page_html,
                            eurovoc_classifiers,
                            full_text,
                        ) = self.__get_full_document(
                            doc_info["link"].replace(
                                "AUTO", f"{self.lang.upper()}/ALL"
                            ),
                            max_retries=max_retries,
                            log_errors=log_errors,
                            scrape=save_data,
                            label_types=label_types,
                        )

                        self.__save_checkpoint(
                            f"{directory}/checkpoint.json",
                            endpoint,
                            f"{doc_info['link'].replace('AUTO', f'{self.lang.upper()}/ALL')}",
                        )

                        if save_html and page_html != "":
                            self.__save_file(
                                f"{directory}/docsHTML/{dirterm}/{doc_id}.html",
                                bytes(page_html, encoding="utf-8"),
                            )

                        documents[term][doc_id][
                            "eurovoc_classifiers"
                        ] = eurovoc_classifiers
                        documents[term][doc_id]["full_text"] = full_text
                        sleep(sleep_time)

                    if skip_count == len(documents_in_page):
                        sleep(sleep_time)

                    if total_pages == 0:
                        total_pages = int(
                            soup.find("i", class_="fa fa-angle-double-right")
                            .parent["href"]
                            .split("&page=")[1]
                            if soup.find("i", class_="fa fa-angle-double-right")
                            else page
                        )

                    if soup.find("i", class_="fa fa-angle-right"):
                        page += 1
                    else:
                        if page < total_pages or total_pages == 0:
                            logging.error(
                                f"Error fetching search page {page}. Cooldown..."
                            )
                            sleep(60)
                            count += 1
                            continue

                        logging.info(f"Reached end of search results at page {page}")
                        end = True
                        page = 1

                    if page % 10 == 0:
                        logging.info(
                            f"Currently at {page}/{total_pages} pages for {term}..."
                        )
                else:
                    if self.r.status_code == 504:
                        logging.error(
                            f"Error fetching page {endpoint}. Status code: {self.r.status_code}, cooldown..."
                        )
                        sleep(60)
                    elif count < max_retries:
                        logging.error(
                            f"Error fetching page {endpoint}. Status code: {self.r.status_code}, trying again"
                        )
                        count += 1
                        sleep(count)
                    else:
                        logging.error(
                            f"Error fetching page {endpoint}. Status code: {self.r.status_code}, skipping"
                        )
                        if log_errors:
                            with open(
                                path.realpath(path.join(directory, "errors.txt")),
                                "a",
                                encoding="utf-8",
                            ) as fp:
                                fp.write(
                                    endpoint + f" unreachable at {datetime.now()}\n"
                                )
                        end = True
                        page = 1

            logging.info(
                f"Scraping for {term} completed.\n- Documents scraped: {len(documents[term])}\n"
                f"- Documents without eurovoc classifiers: {len([doc for doc in documents[term] if len(documents[term][doc]['eurovoc_classifiers']) == 0])}\n"
                f"- Average number of Eurovoc classifiers per document: {sum([len(documents[term][doc]['eurovoc_classifiers']) for doc in documents[term]])/len(documents[term]) if len(documents[term]) > 0 else 0}"
            )
            if save_data:
                with gzip.open(
                    f"{directory}/{dirterm}.json.gz", "wt", encoding="utf-8"
                ) as fp:
                    json.dump(documents[term], fp, ensure_ascii=False)

                del documents[term]

        return documents

    def get_number_per_year(self):
        """
        Get the number of documents per year

        :return: number of documents per year
        """
        endpoint = self.base_url + f"&qid={int(datetime.now().timestamp())}" + "&page=1"
        self.r = requests.get(endpoint, timeout=60)
        if self.r.ok:
            soup = BeautifulSoup(self.r.text, "lxml")
            number_per_year = {}
            for year in soup.find("form", id="DD_YEAR_Form").parent.parent.find_all(
                "li"
            )[:-1]:
                number_per_year[year.find("a")["id"].split("_")[-1]] = int(
                    year.find("a").find("span").text.split("(")[1].split(")")[0]
                )
            for year in soup.find("select", id="DD_YEAR").find_all("option"):
                if year["value"] != "":
                    number_per_year[year["value"]] = int(
                        year.text.split("(")[1].split(")")[0]
                    )
            return number_per_year
        else:
            logging.error(
                f"Error fetching page {endpoint}. Status code: {self.r.status_code}"
            )
            return {}

    def get_available_years(self):
        """
        Get the years available on EUR-lex.
        NOTE: some years may not have any documents, but have been added in case the documents are added in the future

        :return: set of years
        """
        return self.year_list

    def get_available_categories(self):
        """
        Get the document types available on EUR-lex

        :return: dictionary of document types
        """
        return self.document_types

    def get_available_languages(self):
        """
        Get the languages available on EUR-lex

        :return: set of languages
        """
        return self.lang_set

    def get_single_document(self, endpoint, max_retries=10, label_types="TC"):
        """
        Get the Eurovoc classifiers and full text of a single document

        :param endpoint: document endpoint
        :param max_retries: max number of retries.
        :param label_types: label types to scrape.
        :return: dictionary of document information
        """
        page_html, eurovoc_classifiers, full_text = self.__get_full_document(
            endpoint, max_retries, label_types=label_types
        )
        soup = BeautifulSoup(page_html, "lxml")
        return {
            "link": endpoint,
            "eurovoc_classifiers": eurovoc_classifiers,
            "full_text": full_text,
            "title": self.__clean_text(
                soup.find("p", {"id": "originalTitle"}).text.strip()
                if soup.find("p", {"id": "originalTitle"})
                else ""
            ),
        }

    def get_documents_by_category(
        self,
        categories=[],
        log_errors=True,
        save_html=False,
        save_data=False,
        directory="./",
        max_retries=10,
        sleep_time=0,
        n=0,
        resume=False,
        skip_existing=True,
        label_types="TC",
    ):
        """
        Scrape all the documents for the given categories
        NOTE: the 'n' parameter is not implemented yet

        :param categories: list of categories to scrape.
        :param log_errors: whether to log errors in a file.
        :param save_html: whether to save the html of each document.
        :param save_data: whether to save the scraped data of each category in its own file. Pass 'False' if you want to handle the saving yourself.
        :param directory: directory to save the scraped data.
        :param max_retries: maximum number of retries for each page, both search pages and individual documents.
        :param sleep_time: time to sleep between each document request.
        :param n: number of documents to scrape.
        :param resume: whether to resume scraping from the last checkpoint.
        :param skip_existing: whether to skip the documents that have already been scraped.
        :param label_types: which labels to extract.
        :return: dictionary of documents
        """
        directory = f"{directory}/{self.lang}"
        makedirs(directory, exist_ok=True)
        self.__set_cookies()

        if len(categories) == 0:
            categories = self.document_types.keys()

        search_term = "&DB_TYPE_OF_ACT=&typeOfActStatus=OTHER&FM_CODED="
        resume_params = None

        if resume:
            try:
                with open(f"{directory}/checkpoint.json", "r", encoding="utf-8") as fp:
                    checkpoint = json.load(fp)
            except FileNotFoundError:
                raise Exception("Checkpoint unavailable. Please set 'resume' to False")

            if "FM_CODED" in checkpoint["last_search_endpoint"]:
                resume_params = {
                    "page": checkpoint["last_search_endpoint"].split("&page=")[1],
                    "term": checkpoint["last_search_endpoint"]
                    .split("&FM_CODED=")[1]
                    .split("&")[0],
                }
                logging.info(
                    f"Resuming scraping from {checkpoint['last_search_endpoint']}"
                )
            else:
                raise Exception("Cannot resume scraping from this checkpoint")

        return self.__get_documents_search(
            search_term,
            categories,
            log_errors,
            save_html,
            save_data,
            directory,
            max_retries,
            sleep_time,
            n,
            "category",
            resume_params,
            skip_existing,
            label_types,
        )

    def get_documents_by_year(
        self,
        years=[],
        log_errors=True,
        save_html=False,
        save_data=False,
        directory="./",
        max_retries=10,
        sleep_time=0,
        n=0,
        resume=False,
        skip_existing=True,
        label_types="TC",
    ):
        """
        Get all the documents for the given years
        NOTE: the 'n' parameter is not implemented yet

        :param years: years to scrape.
        :param log_errors: whether to log errors in a file, allowing the user to check the faulty URLs.
        :param save_html: whether to save the html of each scraped page in its own file.
        :param save_data: whether to save the scraped data of each year in its own file. Pass 'False' if you want to handle the saving yourself.
        :param directory: directory to save the scraped data.
        :param max_retries: maximum number of retries for each page, both search pages and individual documents.
        :param sleep_time: time to sleep between each document request.
        :param n: number of documents to scrape.
        :param resume: whether to resume scraping from the last saved year.
        :param skip_existing: whether to skip the documents that have already been scraped.
        :param label_types: which labels to extract.
        :return: dictionary of documents
        """
        directory = f"{directory}/{self.lang}"
        makedirs(directory, exist_ok=True)
        self.__set_cookies()

        years = (
            self.year_list
            if years == ""
            else [str(year) for year in PageRange(years).pages]
        )

        search_term = "&DD_YEAR="
        resume_params = None

        if resume:
            try:
                with open(f"{directory}/checkpoint.json", "r", encoding="utf-8") as fp:
                    checkpoint = json.load(fp)
            except FileNotFoundError:
                raise Exception("Checkpoint unavailable. Please set 'resume' to False")

            if "DD_YEAR" in checkpoint["last_search_endpoint"]:
                resume_params = {
                    "page": checkpoint["last_search_endpoint"].split("&page=")[1],
                    "term": checkpoint["last_search_endpoint"]
                    .split("&DD_YEAR=")[1]
                    .split("&")[0],
                }
                logging.info(
                    f"Resuming scraping from {checkpoint['last_search_endpoint']}"
                )
            else:
                raise Exception("Cannot resume scraping from this checkpoint")

        return self.__get_documents_search(
            search_term,
            years,
            log_errors,
            save_html,
            save_data,
            directory,
            max_retries,
            sleep_time,
            n,
            "year",
            resume_params,
            skip_existing,
            label_types,
        )

    def scrape_local_core(self, info):
        """
        Core of the local scraping process for a single document.

        :param info: tuple containing the file name and the directory
        :return: dictionary containing the scraped data
        """
        file, directory, label_types = info
        to_rtn = {}
        try:
            with gzip.open(path.join(directory, file), "rb") as fp:
                page_html = fp.read()
        except Exception as e:
            print()
            logging.error(f"Error while reading {file}: {e}")
            return to_rtn

        soup = BeautifulSoup(page_html, "lxml")
        doc_id_generator = file.split(".html")[0].split("-", maxsplit=1)
        try:
            doc_id = doc_id_generator[1]
        except:
            logging.error(f"Error while reading {file}. Invalid file name.")
            return to_rtn
        to_rtn[doc_id] = {
            "title": self.__clean_text(
                soup.find("p", {"id": "originalTitle"}).text.strip()
                if soup.find("p", {"id": "originalTitle"})
                else ""
            ),
            "link": f"https://eur-lex.europa.eu/legal-content/AUTO/?uri={doc_id_generator[0]}:{doc_id}",
        }
        eurovoc_classifiers, full_text = self.__scrape_page(page_html.decode("utf-8"), label_types)

        to_rtn[doc_id]["eurovoc_classifiers"] = eurovoc_classifiers
        to_rtn[doc_id]["full_text"] = full_text
        return to_rtn

    def get_documents_local(
        self, directory, json_folder=None, years=[], language="", label_types="TC"
    ):
        """
        Scrape information from local files

        :param directory: main directory of the files to scrape
        :param save_data: whether to save the data in a file in the same directory of the files.
        :param years: range of years to scrape.
        :param language: language of the documents to scrape.
        :param label_types: which labels to extract.
        """
        if not directory:
            raise ValueError("No directory specified")
        if not path.isdir(directory):
            raise ValueError("Directory not found")

        out_dir = path.join(directory, language, "extracted")
        if json_folder:
            out_dir = path.join(json_folder, language)
        makedirs(out_dir, exist_ok=True)

        years = (
            self.year_list
            if years == ""
            else [str(year) for year in PageRange(years).pages]
        )

        for year in years:
            documents = {}

            dir_scrape = path.join(directory, language, "docsHTML", str(year))

            if not path.isdir(dir_scrape):
                print(f"Directory {dir_scrape} not found. Skipping...")

            tqdm.write(f"Scraping documents in {dir_scrape}...")
            for file in tqdm(listdir(dir_scrape)):
                if file.endswith(".gz"):
                    documents.update(
                        self.scrape_local_core((file, dir_scrape, label_types))
                    )

            tqdm.write(
                f"Scraping completed.\n- Documents scraped: {len(documents)}\n- Documents without eurovoc classifiers: {len([doc for doc in documents if len(documents[doc]['eurovoc_classifiers']) == 0])}\n- Average number of Eurovoc classifiers per document: {sum([len(documents[doc]['eurovoc_classifiers']) for doc in documents])/len(documents)}"
            )

            with gzip.open(
                path.realpath(path.join(out_dir, str(year) + ".json.gz")),
                "wt",
                encoding="utf-8",
            ) as fp:
                json.dump(documents, fp, ensure_ascii=False)

    def get_documents_local_multiprocess(
        self,
        directory,
        json_folder=None,
        cpu_count=2,
        years=[],
        language="",
        label_types="TC",
    ):
        """
        Scrape information from local files using multiprocessing

        :param directory: main directory of the files to scrape
        :param save_data: whether to save the data in a file in the same directory of the files.
        :param cpu_count: number of cores to use. Default: 2
        :param years: list of years to create the range to scrape.
        :param language: language of the documents to scrape.
        :param label_types: which labels to extract.
        """
        if not directory:
            raise ValueError("No directory specified")
        if not path.isdir(directory):
            raise ValueError("Directory not found")

        out_dir = path.join(directory, language, "extracted")
        if json_folder:
            out_dir = path.join(json_folder, language)
        makedirs(out_dir, exist_ok=True)

        years = (
            self.year_list
            if years == ""
            else [str(year) for year in PageRange(years).pages]
        )

        for year in years:
            documents = {}

            dir_scrape = path.join(directory, language, "docsHTML", year)

            if not path.isdir(dir_scrape):
                print(f"Directory {dir_scrape} not found. Skipping...")

            tqdm.write(f"Scraping documents in {dir_scrape}...")
            inputs = [
                (file, dir_scrape, label_types)
                for file in listdir(dir_scrape)
                if file.endswith(".gz")
            ]

            with Pool(cpu_count) as p:
                scraped = list(
                    tqdm(p.imap(self.scrape_local_core, inputs), total=len(inputs))
                )

            for doc in scraped:
                documents.update(doc)

            tqdm.write(
                f"Scraping completed.\n- Documents scraped: {len(documents)}\n- Documents without eurovoc classifiers: {len([doc for doc in documents if len(documents[doc]['eurovoc_classifiers']) == 0])}\n- Average number of Eurovoc classifiers per document: {sum([len(documents[doc]['eurovoc_classifiers']) for doc in documents])/len(documents)}"
            )

            with gzip.open(
                path.realpath(path.join(out_dir, str(year) + ".json.gz")),
                "wt",
                encoding="utf-8",
            ) as fp:
                json.dump(documents, fp, ensure_ascii=False)
