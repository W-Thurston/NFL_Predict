import yaml
from argparse import ArgumentParser

from model_pipelines.PFR.pfr_model_builder import pfr_model_builder

def main():

    print()
    print("Data Pipeline for Pro Football Reference Scraping and Feature Generation")
    ap = ArgumentParser()
    # ap.add_argument("--config_path", help="Path to yaml config file.")
    ap.add_argument("--ELO_only")
    ap.add_argument("--eval_elo")

    args = ap.parse_args()

    # with open(args.config_path) as file:
    #     config_dict = yaml.load(file, Loader=yaml.FullLoader)

    ## Produce Elo based reccommendations ##
    if args.ELO_only:
        print("> Producing ELO only Reccommendations -- START")
        ## Instantiate PFR Model Builder
        PFR_Model_Builder_object = pfr_model_builder()
        
        ## Produce Elo based predictions
        PFR_Model_Builder_object.predict_elo_only()
        print("> Producing ELO only Reccommendations -- END")
        print()

    ## Collect Data ##
    if args.eval_elo:
        print("> Evaluating ELO only Reccommendations -- START")
        ## Instantiate PFR Model Builder
        PFR_Model_Builder_object = pfr_model_builder()
        
        ## Evaluate Elo based predictions
        print("> By Year:")
        PFR_Model_Builder_object.evaluate_elo_only(time_period='YEAR')
        print("> By Week:")
        PFR_Model_Builder_object.evaluate_elo_only(time_period='WEEK')
        print("> Evaluating ELO only Reccommendations -- END")
        print()


if __name__ == '__main__':
    """
    "Usage:"
    python PFR_model_pipeline_run.py
        --ELO_only 0
        --eval_elo 0
    """
    main()