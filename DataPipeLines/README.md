# Data Pipelines
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Technologies Used
- Airflow  1.10.2
- Redshift
- S3


## Setup
- Create IAM user
- Create Redshift cluster in us-west region
- Update connection variables for AWS and Redshift in Apache Airflow
- Write your dag and operators
- Run dag

## Project Status
Project is: _complete_


## Contact
Created by Abhay Krishan Rastogi!

## Results From Redshift Cluster
![Screen Shot 2022-06-29 at 10 08 48 pm](https://user-images.githubusercontent.com/101382920/176433202-e0878731-5820-4ad5-b43f-9f203b6ece0b.png)
![Screen Shot 2022-06-29 at 10 13 07 pm](https://user-images.githubusercontent.com/101382920/176433595-dd44079a-708c-4fe6-a3ab-198ba49adbfa.png)
![Screen Shot 2022-06-29 at 10 13 25 pm](https://user-images.githubusercontent.com/101382920/176433607-aadd07b6-6d25-47bd-86c5-3a7f3e2b5546.png)

## Screenshot From Graph View
![Screen Shot 2022-06-29 at 10 15 59 pm](https://user-images.githubusercontent.com/101382920/176434160-5bafbbb7-3195-4ae3-a749-b0740235f57f.png)


