"""
get endpoint and arn from redshift cluster.
"""

import boto3
import configparser

def main():
    """This is the main function, it will start with this module
    and get endpint and arn from redshift cluster on aws.
    
    Args:
        None
    Returns:
        None
    """

    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY = config.get('AWS','KEY')
    SECRET = config.get('AWS','SECRET')

    DB_CLUSTER_IDENTIFIER = config.get("CLUSTER","DB_CLUSTER_IDENTIFIER")

    redshift = boto3.client (
        'redshift',
        region_name='us-west-2',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )

    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DB_CLUSTER_IDENTIFIER)['Clusters'][0]

    DB_ENDPOINT = myClusterProps['Endpoint']['Address']
    DB_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']

    print("DB_ENDPOINT :: ", DB_ENDPOINT)
    print("DB_ROLE_ARN :: ", DB_ROLE_ARN)

if __name__ == '__main__':
    main()