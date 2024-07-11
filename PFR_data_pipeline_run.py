import yaml
from argparse import ArgumentParser

from src.data_pipelines.PFR.pfr_data_collector import PFR_Data_Collector
from src.data_pipelines.PFR.model_input_feature_builder import model_input_feature_builder

def main():

    print()
    print("Data Pipeline for Pro Football Reference Scraping and Feature Generation")
    ap = ArgumentParser()
    # ap.add_argument("--config_path", help="Path to yaml config file.")
    ap.add_argument("--collect_historical_PFR_data", nargs='+', type=str, help="all_data: bool->False & A year: str-> '2023' ")
    ap.add_argument("--clean_historical_PFR_data")
    ap.add_argument("--collect_upcoming_data")
    ap.add_argument("--clean_upcoming_PFR_data")
    ap.add_argument("--build_model_input_features", nargs='+', type=str, help="all_data: bool->False")
    ap.add_argument("--collect_weather_data", nargs='+', type=str, help="year: str->'2023-2024'")
    ap.add_argument("--draftkings_odds")

    args = ap.parse_args()

    # with open(args.config_path) as file:
    #     config_dict = yaml.load(file, Loader=yaml.FullLoader)
    with open('configs/config.yaml') as file:
        config_dict = yaml.load(file, Loader=yaml.FullLoader)

    ## Collect Data ##
    if args.collect_historical_PFR_data:
        print("> Collecting Historical PFR Data -- START")
        ## Instantiate PFR Scraper
        PFR_Data_Collector_object = PFR_Data_Collector()
        
        ## Run PFR Scraper
        PFR_Data_Collector_object.fetch_historical_data(all_data = eval(args.collect_historical_PFR_data[0]), scrape_year = args.collect_historical_PFR_data[1])
        print("> Collecting Historical PFR Data -- END")
        print()

    ## Clean historical Data ##
    if args.clean_historical_PFR_data:
        print("> Cleaning PFR Scraped data -- START")
        ## Instantiate PFR Scraper
        PFR_Data_Collector_object = PFR_Data_Collector()

        ## Clean PFR Scraped data
        PFR_Data_Collector_object.clean_historical_data()
        print("> Cleaning PFR Scraped data -- END")
        print()

    if args.collect_upcoming_data:
        print("> Collecting the Upcoming Schedule Data -- START")
        ## Instantiate PFR Scraper
        PFR_Data_Collector_object = PFR_Data_Collector()
        
        ## Run PFR Scraper
        PFR_Data_Collector_object.fetch_upcoming_schedule_data()
        print("> Collecting the Upcoming Schedule Data -- END")
        print()

    ## Clean upcoming schedule Data ##
    if args.clean_upcoming_PFR_data:
        print("> Cleaning PFR Upcoming Schedule Scraped data -- START")
        ## Instantiate PFR Scraper
        PFR_Data_Collector_object = PFR_Data_Collector()

        ## Clean PFR Scraped data
        PFR_Data_Collector_object.clean_upcoming_schedule_data()
        print("> Cleaning PFR Upcoming Schedule Scraped data -- END")
        print()
        

    ## Build modeling input data ##
    if args.build_model_input_features:
        print("> Building model input features -- START")
        ## Instantiate model input feature builder
        model_input_feature_builder_object = model_input_feature_builder()

        ## Build base_modeling_file
        model_input_feature_builder_object.prep_data_modeling_file()
        
        ## Create Elo data and append to base modeling file
        model_input_feature_builder_object.handle_elo_data(all_data=eval(args.build_model_input_features[0]))

        ## Create location based features
        model_input_feature_builder_object.handle_location_data(all_data=eval(args.build_model_input_features[0]))

        print("> Building model input features -- END")
        print()

    if args.collect_weather_data:
        print("> Collect Weather Data -- START")
        ## Instantiate PFR Scraper
        PFR_Data_Collector_object = PFR_Data_Collector()

        ## Pull weather data
        PFR_Data_Collector_object.pull_weather_data(year=args.collect_weather_data[0], OWM_API_KEY=config_dict['OWM_API_KEY'])
        print("> Collect Weather Data -- END")
        print()

    ## Pull most recent DraftKings Odds
    if args.draftkings_odds:
        print("> Collect DraftKings odds -- START")
        ## Instantiate Data Collector
        PFR_Data_Collector_object = PFR_Data_Collector()

        ## Pull DraftKings Odds
        PFR_Data_Collector_object.pull_dk_sportsbook_odds()
        print("> Collect DraftKings odds -- END")

if __name__ == '__main__':
    """
    Usage:
    python PFR_data_pipeline_run.py
        --collect_historical_PFR_data False '2023'
        --clean_historical_PFR_data 0
        --collect_upcoming_data 0
        --clean_upcoming_PFR_data 0
        --build_model_input_features False
        --collect_weather_data '2023-2024'
        --draftkings_odds 0
    """
    main()