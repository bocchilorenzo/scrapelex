import argparse
from scraper import EURlexScraper
from pprint import pprint

if __name__ == "__main__":
    #fmt: off
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--language", type=str, default="it", help="Language to scrape.")
    parser.add_argument("--year", type=str, default="", help="Years to scrape.")
    parser.add_argument("--category", type=str, default="", help="Categories to scrape.")
    parser.add_argument("--label_types", type=str, default="TC", help="Label types to scrape. Use comma separated values for multiple types. Accepted values: TC (Thesaurus Concept), MT (Micro Thesaurus), DO (Domain).")
    parser.add_argument("--save_data", default=False, action="store_true", help="Whether to save the scraped data in a JSON file for the year.")
    parser.add_argument("--json_folder", metavar="FOLDER", default=None, help="JSON folder where to save data.")
    parser.add_argument("--save_html", default=False, action="store_true", help="Whether to save the html of each scraped page in its own gzipped file.")
    parser.add_argument("--resume", default=False, action="store_true", help="Use a previous checkpoint to resume scraping.")
    parser.add_argument("--clean", default=False, action="store_true", help="Scrape all the documents, ignoring the ones already downloaded.")
    parser.add_argument("--get_number", default=False, action="store_true", help="Get the number of documents available per year for the specified language.")
    parser.add_argument("--scrape_local", default=False, action="store_true", help="Scrape pages from the local directory instead of the web.")
    parser.add_argument("--multi_core", default=False, action="store_true", help="Use multiple cores to scrape the data. Only works for local scraping.")
    parser.add_argument("--cpu_count", type=int, default=2, help="Number of cores to use for local scraping in case of multicore.")
    parser.add_argument("--directory", type=str, default="./eurlexdata/", help="Directory for the saved data.")
    parser.add_argument("--max_retries", type=int, default=10, help="Maximum number of retries for each page, both search pages and individual documents.")
    parser.add_argument("--sleep_time", type=int, default=1, help="Sleep time between document requests.")
    parser.add_argument("--log_level", type=int, default=2, help="Log level: 0 = errors only, 1 = previous + warnings, 2 = previous + general information.")
    parser.add_argument("--get_categories", default=False, action="store_true", help="Show the available categories.")
    parser.add_argument("--get_languages", default=False, action="store_true", help="Show the available languages.")
    parser.add_argument("--get_years", default=False, action="store_true", help="Show the available years.")

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

    if args.get_number:
        print("Lookup started...")
        docs = scraper.get_number_per_year()
        print("Year\tNumber of documents")
        for year in docs:
            print(f"{year}\t{docs[year]}")
        exit()

    if args.scrape_local:
        if args.multi_core:
            if args.cpu_count < 1:
                print("Invalid core count. Using 1 core.")
                args.cpu_count = 1
            if args.year == "":
                raise BaseException("You must specify at least a year when extracting from local files.")
            documents = scraper.get_documents_local_multiprocess(
                directory=args.directory,
                json_folder=args.json_folder,
                cpu_count=args.cpu_count,
                years=args.year,
                language=args.language,
                label_types=args.label_types,
            )
        else:
            scraper.get_documents_local(
                directory=args.directory,
                json_folder=args.json_folder,
                years=args.year,
                language=args.language,
                label_types=args.label_types,
            )
    else:
        if args.year == "":
            if args.category == "":
                documents = scraper.get_documents_by_year(
                    years=[],
                    save_data=args.save_data,
                    save_html=args.save_html,
                    directory=args.directory,
                    resume=args.resume,
                    max_retries=args.max_retries,
                    sleep_time=args.sleep_time,
                    skip_existing=not(args.clean),
                    label_types=args.label_types,
                )
            else:
                documents = scraper.get_documents_by_category(
                    categories=args.category.split(","),
                    save_data=args.save_data,
                    save_html=args.save_html,
                    directory=args.directory,
                    resume=args.resume,
                    max_retries=args.max_retries,
                    sleep_time=args.sleep_time,
                    skip_existing=not(args.clean),
                    label_types=args.label_types,
                )
        else:
            if args.category != "":
                raise("You can't specify both a category and a year.")
            documents = scraper.get_documents_by_year(
                years=args.year,
                save_data=args.save_data,
                save_html=args.save_html,
                directory=args.directory,
                resume=args.resume,
                max_retries=args.max_retries,
                sleep_time=args.sleep_time,
                skip_existing=not(args.clean),
                label_types=args.label_types,
            )
    
