# Sparkify Analytics

Sparkify wants to migrate their analytics , on AWS for a fully managed environment on cloud and chose for Amazon Redshift.

#### Objective:

Build a Redshift cluster, extract all needed data from S3 on AWS, import it on staging tables on Redshift and then, load all cleaned data to final tables on a star schema model.

#### Pre-requisites:
Create a AWS Account and create a IAMUSER with AdministratorAccess.

#### Datasets
- Song data: `s3://udacity-dend/song_data`
- Log Data: `s3://udacity-dend/log_data`

#### Scripts: 
- dwh.cfg - Configuration file with credentials, cluster and file parameters.
- create_cluster.py - Script for Redshift cluster creation.
- create_tables.py - Script for create all needed tables.
- etl.py - Script for copy datasets from S3, import data to Redshift and load final tables.
- delete_cluster.py - Script for Redshift cluster deletion.
- get_arn.py - Script for get cluster ARN on AWS.

## Running the scripts:
- Configure the key and secret.
- Run the create_cluster.py followed by get_arn.py 
- Note the `endpoint` and edit the dwh.cfg
- set `DB_ROLE_ARN` in dwh.cfg
- create the table using below:
`python create_tables.py`
- once tables are created run the below:
`python etl.py`

## Authors
The project and data were created and provisioned by udacity.com


