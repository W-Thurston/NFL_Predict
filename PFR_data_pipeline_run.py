import yaml
from argparse import ArgumentParser

from data_pipelines.PFR.pfr_data_collector import PFR_Data_Collector
from data_pipelines.PFR.model_input_feature_builder import model_input_feature_builder

def main():

    print()
    print("Data Pipeline for Pro Football Reference Scraping and Feature Generation")
    ap = ArgumentParser()
    # ap.add_argument("--config_path", help="Path to yaml config file.")
    ap.add_argument("--collect_PFR_data")
    ap.add_argument("--clean_PFR_data")
    ap.add_argument("--build_model_input_features")

    args = ap.parse_args()

    # with open(args.config_path) as file:
    #     config_dict = yaml.load(file, Loader=yaml.FullLoader)

    ## Collect Data ##
    if args.collect_PFR_data:
        print("> Collecting PFR Data -- START")
        ## Instantiate PFR Scraper
        PFR_Data_Collector_object = PFR_Data_Collector()
        
        ## Run PFR Scraper
        PFR_Data_Collector_object.fetch_data()
        print("> Collecting PFR Data -- END")
        print()

    ## Collect Data ##
    if args.clean_PFR_data:
        print("> Cleaning PFR Scraped data -- START")
        ## Instantiate PFR Scraper
        PFR_Data_Collector_object = PFR_Data_Collector()

        ## Clean PFR Scraped data
        PFR_Data_Collector_object.clean_data()
        print("> Cleaning PFR Scraped data -- END")
        print()
        

    ## Build modeling input data ##
    if args.build_model_input_features:
        print("> Building model input features -- START")
        ## Instantiate model input feature builder
        model_input_feature_builder_object = model_input_feature_builder()

        ## Build base_modeling_file
        model_input_feature_builder_object.prep_data_modeling_file()
        
        ## Create Elo data and append to base modeling file
        model_input_feature_builder_object.handle_elo_data()

        ## Create location based features
        model_input_feature_builder_object.handle_location_data()

        print("> Building model input features -- END")
        print()

        


if __name__ == '__main__':
    """
    "Usage:"
    python PFR_data_pipeline_run.py
        --collect_PFR_data 0
        --clean_PFR_data 0
        --build_model_input_features 0
    """
    main()