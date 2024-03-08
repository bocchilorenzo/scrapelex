# ScrapeLex
Multilingual EUR-Lex scraper. This is a Python package that allows you to scrape the EUR-Lex website and download (almost) all the documents in a given language. It is based on the [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/) library.

## Requirements

The package requires Python 3.6 or higher. Make sure to also install the requrirements listed in the `requirements.txt` file by running:

```bash
pip install -r requirements.txt
```

## Usage

Clone the repository and run the `main.py` file. By default, if no year or category is given, it will scrape year by year starting from current year - 1 and going back to 1800. The available arguments are:

```
usage: main.py [-h] [--language LANGUAGE] [--year YEAR] [--category CATEGORY] [--label_types LABEL_TYPES] [--save_data] [--json_folder FOLDER] [--save_html] [--resume] [--clean] [--get_number] [--scrape_local] [--multi_core] [--cpu_count CPU_COUNT] [--directory DIRECTORY] [--max_retries MAX_RETRIES] [--sleep_time SLEEP_TIME] [--log_level LOG_LEVEL] [--get_categories] [--get_languages] [--get_years]

optional arguments:
  -h, --help            show this help message and exit
  --language LANGUAGE   Language to scrape. (default: it)
  --year YEAR           Years to scrape. (default: )
  --category CATEGORY   Categories to scrape. (default: )
  --label_types LABEL_TYPES
                        Label types to scrape. Use comma separated values for multiple types. Accepted values: TC (Thesaurus Concept), MT (Micro Thesaurus), DO (Domain). (default: TC)
  --save_data           Whether to save the scraped data in a JSON file for the year. (default: False)
  --json_folder FOLDER  JSON folder where to save data. (default: None)
  --save_html           Whether to save the html of each scraped page in its own gzipped file. (default: False)
  --resume              Use a previous checkpoint to resume scraping. (default: False)
  --clean               Scrape all the documents, ignoring the ones already downloaded. (default: False)
  --get_number          Get the number of documents available per year for the specified language. (default: False)
  --scrape_local        Scrape pages from the local directory instead of the web. (default: False)
  --multi_core          Use multiple cores to scrape the data. Only works for local scraping. (default: False)
  --cpu_count CPU_COUNT
                        Number of cores to use for local scraping in case of multicore. (default: 2)
  --directory DIRECTORY
                        Directory for the saved data. (default: ./eurlexdata/)
  --max_retries MAX_RETRIES
                        Maximum number of retries for each page, both search pages and individual documents. (default: 10)
  --sleep_time SLEEP_TIME
                        Sleep time between document requests. (default: 1)
  --log_level LOG_LEVEL
                        Log level: 0 = errors only, 1 = previous + warnings, 2 = previous + general information. (default: 2)
  --get_categories      Show the available categories. (default: False)
  --get_languages       Show the available languages. (default: False)
  --get_years           Show the available years. (default: False)
```
