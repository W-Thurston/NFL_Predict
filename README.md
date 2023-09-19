# NFL_Predict
The beginnings of a project to predict NFL scores and potentially fantasy football output


How to update data:
    If its your first time, change arguments to fit your needs:
        python PFR_data_pipeline_run.py \
                --collect_historical_PFR_data True '2023' \
                --clean_historical_PFR_data 0 \
                --collect_upcoming_data 0 \
                --clean_upcoming_PFR_data 0  \
                --build_model_input_features 0  \
                --draftkings_odds 0 

        python PFR_model_pipeline_run.py \
            --ELO_only '2023-2024' 3 \
            --ranks_and_betting '2023-2024' 2

    If you are updating data for the week, run this once data is updated on Pro Football Reference:
        python PFR_data_pipeline_run.py \
                    --collect_historical_PFR_data False '2023' \
                    --clean_historical_PFR_data 0 \
                    --build_model_input_features 0  \
                    --draftkings_odds 0
        
        python PFR_model_pipeline_run.py \
            --ELO_only '2023-2024' 3 \
            --ranks_and_betting '2023-2024' 2

        