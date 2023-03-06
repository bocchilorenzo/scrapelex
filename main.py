import argparse
from scraper import EURlexScraper
from pprint import pprint

if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--language", type=str, default="it", help="Language to scrape.")
    parser.add_argument(
        "--year", type=str, default="", help="Years to scrape."
    )
    parser.add_argument(
        "--category", type=str, default="", help="Categories to scrape."
    )
    parser.add_argument(
        "--save_data",
        default=False,
        action="store_true",
        help="Whether to save the scraped data in a json file for the year.",
    )
    parser.add_argument(
        "--save_html",
        default=False,
        action="store_true",
        help="Whether to save the html of each scraped page in its own gzipped file.",
    )
    parser.add_argument(
        "--resume",
        default=False,
        action="store_true",
        help="Use a previous checkpoint to resume scraping.",
    )
    parser.add_argument(
        "--scrape_local",
        default=False,
        action="store_true",
        help="Scrape pages from the local directory instead of the web.",
    )
    parser.add_argument(
        "--directory",
        type=str,
        default="./eurlexdata/",
        help="Directory to save the scraped data, or to process in case of local scraping.",
    )
    parser.add_argument(
        "--max_retries",
        type=int,
        default=10,
        help="Maximum number of retries for each page, both search pages and individual documents.",
    )
    parser.add_argument(
        "--log_level",
        type=int,
        default=2,
        help="Log level: 0 = errors only, 1 = previous + warnings, 2 = previous + general information.",
    )
    parser.add_argument(
        "--get_categories",
        default=False,
        action="store_true",
        help="Show the available categories.",
    )
    parser.add_argument(
        "--get_languages",
        default=False,
        action="store_true",
        help="Show the available languages.",
    )
    parser.add_argument(
        "--get_years",
        default=False,
        action="store_true",
        help="Show the available years.",
    )

    args = parser.parse_args()

    scraper = EURlexScraper(lang=args.language, log_level=args.log_level)

    if args.get_categories:
        pprint(scraper.get_available_categories())
        exit()
    
    if args.get_languages:
        pprint(scraper.get_available_languages())
        exit()
    
    if args.get_years:
        pprint(scraper.get_available_years())
        exit()

    if args.scrape_local:
        scraper.get_documents_local(
            directory=args.directory,
            save_data=args.save_data,
        )

    if args.year == "":
        if args.category == "":
            documents = scraper.get_documents_by_year(
                years=[],
                save_data=args.save_data,
                save_html=args.save_html,
                directory=args.directory,
                resume=args.resume,
                max_retries=args.max_retries,
            )
        documents = scraper.get_documents_by_category(
            categories=args.category.split(","),
            save_data=args.save_data,
            save_html=args.save_html,
            directory=args.directory,
            resume=args.resume,
            max_retries=args.max_retries,
        )
    else:
        if args.category != "":
            raise("You can't specify both a category and a year.")
        documents = scraper.get_documents_by_year(
            years=args.year.split(","),
            save_data=args.save_data,
            save_html=args.save_html,
            directory=args.directory,
            resume=args.resume,
            max_retries=args.max_retries,
        )
    
