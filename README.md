# Project Details
This Repository contains all project files, including,
  1. Python .py files for pipelines
     a. config.py
     b. extract_load_pipeline.py
     c. upload_to_s3.py

  2. dbt project folder containing all models for transformations
  3. Tableau Presentation Workbook and Snapshot


This project followed the ELT (Extract, Load and Transform) Model

Extraction was done using Python's SQLAlchemy, Boto3, BotoCore and Pandas. 

CSV file on s3 is read using Boto3 and staged into a Pandas dataframe.

Loading is also done using Python and SQLAlchemy and files were loaded into staging schema. See extract_load_pipeline.py for details.

Transformation was performed using dbt and results posted into analytics schema. See data2bots_transformation for dbt folder.

Analytics models were uploaded into analytics_export folder. See upload_to_s3.py in repository.

Analytics results were then visualized using Tableau. See workbook and screenshot in repository.

Thank you




     
