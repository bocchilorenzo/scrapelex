# scrapelex
Multilingual EURlex scraper. This is a Python package that allows you to scrape the EURlex website and download (almost) all the documents in a given language. It is based on the [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/) library.

## Requirements

The package requires Python 3.6 or higher. Make sure to also install the requrirements listed in the `requirements.txt` file by running:

```bash
pip install -r requirements.txt
```

## Usage

Clone the repository and run the `main.py` file. The available arguments are:

```
usage: main.py [-h] [--language LANGUAGE] [--year YEAR] [--category CATEGORY] [--save_data] [--save_html] [--resume] [--directory DIRECTORY] [--max_retries MAX_RETRIES] [--log_level LOG_LEVEL] [--get_categories] [--get_languages] [--get_years]

optional arguments:
  -h, --help            show this help message and exit
  --language LANGUAGE   Language to scrape. (default: it)
  --year YEAR           Years to scrape. (default: )
  --category CATEGORY   Categories to scrape. (default: )
  --save_data           Whether to save the scraped data in a json file for the year. (default: False)
  --save_html           Whether to save the html of each scraped page in its own gzipped file. (default: False)
  --resume              Use a previous checkpoint to resume scraping. (default: False)
  --directory DIRECTORY
                        Directory to save the scraped data. (default: ./eurlexdata/)
  --max_retries MAX_RETRIES
                        Maximum number of retries for each page, both search pages and individual documents. (default: 10)
  --log_level LOG_LEVEL
                        Log level: 0 = errors only, 1 = previous + warnings, 2 = previous + general information. (default: 2)
  --get_categories      Show the available categories. (default: False)
  --get_languages       Show the available languages. (default: False)
  --get_years           Show the available years. (default: False)
```

## TODO

- [ ] Add year stats, like average number of eurovoc classifiers per document
- [ ] Add the language in the directory structure