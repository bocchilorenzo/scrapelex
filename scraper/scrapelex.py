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

class EURlexScraper:
    def __init__(self, lang="it", log_level=0):
        """
        Initialize the EurLexScraper object

        :param lang: language of the EUR-lex website. Default: it
        :param log_level: logging level. Available values: 0, 1, 2. Default: 0.
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

        alpha3 = languagecodes.iso_639_alpha3("it").strip().upper()
        self.base_url = f"https://eur-lex.europa.eu/search.html?SUBDOM_INIT=ALL_ALL&DTS_SUBDOM=ALL_ALL&DTS_DOM=ALL&lang={self.lang}&locale={self.lang}&type=advanced&wh0=andCOMPOSE%3D{alpha3}%2CorEMBEDDED_MANIFESTATION-TYPE%3Dpdf%3BEMBEDDED_MANIFESTATION-TYPE%3Dpdfa1a%3BEMBEDDED_MANIFESTATION-TYPE%3Dpdfa1b%3BEMBEDDED_MANIFESTATION-TYPE%3Dpdfa2a%3BEMBEDDED_MANIFESTATION-TYPE%3Dpdfx%3BEMBEDDED_MANIFESTATION-TYPE%3Dpdf1x%3BEMBEDDED_MANIFESTATION-TYPE%3Dhtml%3BEMBEDDED_MANIFESTATION-TYPE%3Dxhtml%3BEMBEDDED_MANIFESTATION-TYPE%3Ddoc%3BEMBEDDED_MANIFESTATION-TYPE%3Ddocx"
        self.base_url_year = f"https://eur-lex.europa.eu/search.html?SUBDOM_INIT=ALL_ALL&DTS_SUBDOM=ALL_ALL&DTS_DOM=ALL&lang={self.lang}&locale={self.lang}&type=advanced&wh0=andCOMPOSE%3D{alpha3}%2CorEMBEDDED_MANIFESTATION-TYPE%3Dpdf%3BEMBEDDED_MANIFESTATION-TYPE%3Dpdfa1a%3BEMBEDDED_MANIFESTATION-TYPE%3Dpdfa1b%3BEMBEDDED_MANIFESTATION-TYPE%3Dpdfa2a%3BEMBEDDED_MANIFESTATION-TYPE%3Dpdfx%3BEMBEDDED_MANIFESTATION-TYPE%3Dpdf1x%3BEMBEDDED_MANIFESTATION-TYPE%3Dhtml%3BEMBEDDED_MANIFESTATION-TYPE%3Dxhtml%3BEMBEDDED_MANIFESTATION-TYPE%3Ddoc%3BEMBEDDED_MANIFESTATION-TYPE%3Ddocx"
        self.r = requests.Session()
        self.r.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:108.0) Gecko/20100101 Firefox/108.0",
                "Accept-Language": f"{self.lang},en-US;q=0.7,en;q=0.3",
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
        res = self.r.get(
            f"https://eur-lex.europa.eu/search.html?scope=EURLEX&lang=it&type=quick&qid={int(datetime.now().timestamp())}"
        )
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

    def __scrape_page(self, page_html):
        """
        Utility function to scrape the needed information from the page

        :param page_html: html of the page
        :return: list of eurovoc classifiers and full text of the document
        """
        soup = BeautifulSoup(page_html, "lxml")
        eurovoc_classifiers = []
        full_text = ""

        page_classifiers = soup.find("div", {"id": "PPClass_Contents"})
        if page_classifiers:
            eurovoc_classifiers = [
                classifier.find("a")["href"].split("DC_CODED=")[1].split("&")[0].strip()
                for classifier in page_classifiers.find("ul").find_all("li")
                if classifier.find("a") and "DC_CODED=" in classifier.find("a")["href"]
            ]

        text_element = None
        texteonly = False
        if soup.find("div", id="TexteOnly"):
            text_element = soup.find("div", id="TexteOnly").find("txt_te")
            texteonly = True
        elif soup.find("p", {"class": "oj-doc-ti"}) or soup.find(
            "p", {"class": "doc-ti"}
        ):
            text_element = (
                soup.find("div", {"id": "document1"})
                .find("div", {"class": "tabContent"})
                .find("div")
            )

        if text_element:
            if texteonly:
                full_text += (
                    self.__clean_text(
                        soup.find("div", {"id": "document1"})
                        .find("div", {"class": "tabContent"})
                        .find("strong")
                        .text
                    )
                    + " "
                )

            for child in text_element.children:
                if child.name == "p":
                    full_text += self.__clean_text(child.text) + " "
                elif child.name == "div":
                    for p in child.find_all("p"):
                        full_text += self.__clean_text(p.text) + " "
                elif child.name == "table":
                    for tr in child.find_all("tr"):
                        full_text += self.__clean_text(tr.text) + " "
                elif child.name == "hr":
                    full_text += "[SEP]"

        full_text = full_text.replace("\n", " ")
        full_text = (
            full_text.split("[SEP]", maxsplit=1)[1].replace("[SEP]", "")
            if len(full_text) > 0 and "[SEP]" in full_text
            else full_text
        )
        full_text = sub(" +", " ", full_text).strip()

        return eurovoc_classifiers, full_text

    def __get_full_document(
        self, endpoint, max_retries=10, log_errors=True, directory="./"
    ):
        """
        Extract information from an individual document page from EUR-lex

        :param endpoint: page endpoint
        :param max_retries: max number of retries. Default: 10
        :param log_errors: log errors to a file. Default: True
        :param directory: directory of the error file. Default: current directory
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
                eurovoc_classifiers, full_text = self.__scrape_page(page_html)

            else:
                if self.r.status_code == 404:
                    logging.warning(f"Page {endpoint} not found")
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
        n=0,
        mode="year",
        resume_params=None,
    ):
        """
        General function that scrapes documents from the search page
        NOTE: the 'n' parameter is not implemented yet

        :param terms: list of terms to scrape. Default: all
        :param search_term: search term to use
        :param log_errors: whether to log errors in a file. Default: True
        :param save_html: whether to save the html of each document. Default: False
        :param save_data: whether to save the scraped data of each category in its own file. Pass 'False' if you want to handle the saving yourself. Default: False
        :param directory: directory to save the scraped data. Default: current directory
        :param max_retries: maximum number of retries for each page, both search pages and individual documents. Default: 10
        :param n: number of documents to scrape. Default: 0 (all)
        :param mode: whether to scrape by year or category. Default: year
        :param resume_params: dictionary containing the parameters to resume scraping. Default: None
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
            while not end:
                endpoint = (
                    base_url
                    + search_term
                    + term
                    + f"&qid={int(datetime.now().timestamp())}"
                    + f"&page={page}"
                )
                try:
                    self.r = requests.get(endpoint)
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

                    for doc_id, doc_info in documents_in_page.items():
                        documents[term][doc_id] = doc_info

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
                        sleep(1)

                    if soup.find("i", class_="fa fa-angle-right"):
                        page += 1
                    else:
                        logging.info(f"Reached end of search results at page {page}")
                        end = True
                        page = 1

                    total_pages = (
                        soup.find("i", class_="fa fa-angle-double-right")
                        .parent["href"]
                        .split("&page=")[1]
                        if soup.find("i", class_="fa fa-angle-double-right")
                        else page
                    )
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
                with open(f"{directory}/{dirterm}.json", "w", encoding="utf-8") as fp:
                    json.dump(documents[term], fp, ensure_ascii=False, indent=4)

                del documents[term]

        return documents

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

    def get_single_document(self, endpoint, max_retries=10):
        """
        Get the Eurovoc classifiers and full text of a single document

        :param endpoint: document endpoint
        :param max_retries: max number of retries. Default: 10
        :return: dictionary of document information
        """
        _, eurovoc_classifiers, full_text = self.__get_full_document(
            endpoint, max_retries
        )
        return {
            "link": endpoint,
            "eurovoc_classifiers": eurovoc_classifiers,
            "full_text": full_text,
        }

    def get_documents_by_category(
        self,
        categories=[],
        log_errors=True,
        save_html=False,
        save_data=False,
        directory="./",
        max_retries=10,
        n=0,
        resume=False,
    ):
        """
        Scrape all the documents for the given categories
        NOTE: the 'n' parameter is not implemented yet

        :param categories: list of categories to scrape. Default: all
        :param log_errors: whether to log errors in a file. Default: True
        :param save_html: whether to save the html of each document. Default: False
        :param save_data: whether to save the scraped data of each category in its own file. Pass 'False' if you want to handle the saving yourself. Default: False
        :param directory: directory to save the scraped data. Default: current directory
        :param max_retries: maximum number of retries for each page, both search pages and individual documents. Default: 10
        :param n: number of documents to scrape. Default: 0 (all)
        :param resume: whether to resume scraping from the last checkpoint. Default: False
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
            n,
            "category",
            resume_params,
        )

    def get_documents_by_year(
        self,
        years=[],
        log_errors=True,
        save_html=False,
        save_data=False,
        directory="./",
        max_retries=10,
        n=0,
        resume=False,
    ):
        """
        Get all the documents for the given years
        NOTE: the 'n' parameter is not implemented yet

        :param years: list of years to scrape. Default: all
        :param log_errors: whether to log errors in a file, allowing the user to check the faulty URLs. Default: True
        :param save_html: whether to save the html of each scraped page in its own file. Default: False
        :param save_data: whether to save the scraped data of each year in its own file. Pass 'False' if you want to handle the saving yourself. Default: False
        :param directory: directory to save the scraped data. Default: current directory
        :param max_retries: maximum number of retries for each page, both search pages and individual documents. Default: 10
        :param n: number of documents to scrape. Default: 0 (all)
        :param resume: whether to resume scraping from the last saved year. Default: False
        :return: dictionary of documents
        """
        directory = f"{directory}/{self.lang}"
        makedirs(directory, exist_ok=True)
        self.__set_cookies()

        if len(years) == 0:
            years = self.year_list

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
            n,
            "year",
            resume_params,
        )

    def get_documents_local(self, directory, save_data=False):
        """
        Scrape information from local files

        :param directory: directory of the files to scrape
        :param save_data: whether to save the data in a file in the same directory of the files. Default: False
        :return: dictionary of documents
        """

        if not directory:
            raise ValueError("No directory specified")
        if not path.isdir(directory):
            raise ValueError("Directory not found")

        documents = {}

        tqdm.write(f"Scraping documents in {directory}...")
        for file in tqdm(listdir(directory)):
            if file.endswith(".gz"):
                with gzip.open(path.join(directory, file), "rb") as fp:
                    page_html = fp.read()

                    soup = BeautifulSoup(page_html, "lxml")
                    doc_id = file.split(".html")[0]
                    documents[doc_id] = {
                        "title": self.__clean_text(
                            soup.find("p", {"id": "originalTitle"}).text.strip()
                        ),
                        "link": "https://eur-lex.europa.eu/legal-content/AUTO/?uri=CELEX:"
                        + doc_id,
                    }
                    eurovoc_classifiers, full_text = self.__scrape_page(page_html)

                    documents[doc_id]["eurovoc_classifiers"] = eurovoc_classifiers
                    documents[doc_id]["full_text"] = full_text

        tqdm.write(
            f"Scraping completed.\n- Documents scraped: {len(documents)}\n- Documents without eurovoc classifiers: {len([doc for doc in documents if len(documents[doc]['eurovoc_classifiers']) == 0])}\n- Average number of Eurovoc classifiers per document: {sum([len(documents[doc]['eurovoc_classifiers']) for doc in documents])/len(documents)}"
        )

        if save_data:
            with open(
                path.realpath(path.join(directory, f"documents.json")),
                "w",
                encoding="utf-8",
            ) as fp:
                json.dump(documents, fp, ensure_ascii=False, indent=4)

        return documents
