# MAST30034 Industry Project: Buy Now, Pay Later Project
# Industry Project 18
**Members:**
* Melissa Putri (1389438)
* Zhaoxu Chen (1174019)
* Yuanchen Cai (1301221)
* Ziting Wang (1346793)
* Zihan Wang (1128922)

**Research Goal:** Build a merchant ranking system to determine the top 100 merchants to be included in the Buy Now, Pay Later program.
**Timeline:** 2021 - 2022

# Project Structure
* **data/tables**: Contains all unzipped datasets provided through the LMS, including transactions, consumer, and merchant information.
* **data/curated**: Contains all curated datasets through data preprocessing and modelling.
* **data/external**: Contains all downloaded external datasets that aren't provided through the LMS.
* **scripts**: Contains ETL scripts to download all datasets needed.
* **notebooks**: Contains Jupyter Notebook files used for preprocessing and exploratory data analysis.
* **plots**: Contains images and plots produced to help visualize data distributions and model evaluation.
* **models**: Contains Jupyter Notebook files used to model the ranking system for overall merchant ranking as well as specified ones for several segments. Also contains the metadata for the generated random forest and gradient boosted tree ranking system for evaluation purposes.

# Setup and Requirements
To run this project, ensure that the necessary Python packages are installed. You can install the required packages by running:

pip install -r requirements.txt

Python ver 3.11.5 was used in building this code.

# Pipeline
To run the pipeline please run the following notebooks sequentially.

First, please visit the **scripts** directory, then run:
1. **init.py** : Before running the code, **please insert your Canvas API key in the API_TOKEN variable**, to allow access to the data uploaded within the MAST30034 LMS page. API key should not be pushed to repository for security purposes. This script downloads all relevant merchant, customer, and transaction data.
2. **external_data.py** : Downloads all necessary external datasets.

Then visit the **notebooks** directory, then run:
1. **data_preprocessing_1.ipynb** : Handles missing values and merging all data.
2. **data_preprocessing_2.ipynb** : Aggregates the database with the external dataset containing unemployment rates as well as SA4 codes.
3. **detecting_fraud_ipynb** : Introduces a fraud detection system and flags relevant transactions as fraud, additional preprocessing step to extract information from merchant tags.
4. **exploratory_data_analysis.ipynb** : Produces visualizations of data distributions and important insights.

Then, please visit the **models** directory for the ranking systems:
1. **RF_model.ipynb** : A random forest regressor model to forecast merchant monthly revenue then rank the merchants accordingly.
2. **GBT_model.ipynb** : A gradient boosted tree model to forecast merchant monthly revenue then rank the merchants accordingly.
3. **Logistic_reg.ipynb** : A logistic regression model to forecast merchant fraud probability then rank the merchants accordingly. This model is used to analyze feature correlations.
4. **XGBoost_model.ipynb** : A extreme gradient boosted tree model to forecast merchant fraud probability then rank the merchants accordingly. This model is used to analyze feature correlations.
5. **florist_model.ipynb** : A specific random forest regressor model for the florist segmentation to forecast returning customer ratio then ranking merchants accordingly.
6. **furniture_model.ipynb** : A specific random forest regressor model for the furniture segmentation to forecast yearly total transactions.
7. **computer_model.ipynb** : A specific random forest regressor model for the computer segmentation to forecast annual average order value.

Finally, please re-visit the **notebooks** directory and run **final_summary.ipynb** to retreive the final findings from the ranking systems and see the final ranked merchants of top 100 overall and top 10 within segments.