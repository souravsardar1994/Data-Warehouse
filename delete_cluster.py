"""
It will be delete redshift cluster .
"""

import boto3
import configparser

def main():
    """This is the main function, it will start with this module
    and will be responsible for delete redshift cluster, detach role policy and delete it
    
    """
    
    #01. Importing AWS parameters
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    
    KEY = config.get('AWS','KEY')
    SECRET = config.get('AWS','SECRET')
    
    DB_CLUSTER_IDENTIFIER = config.get("CLUSTER","DB_CLUSTER_IDENTIFIER")
    DB_IAM_ROLE_NAME = config.get("CLUSTER", "DB_IAM_ROLE_NAME")
    
    #02. Creating clients for AWS Services
    redshift = boto3.client (
        'redshift',
        region_name='us-west-2',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )

    iam = boto3.client (
        'iam',
        region_name='us-west-2',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )

    #03. Deleting cluster
    try:
        print('Deleting cluster...')
        redshift.delete_cluster(ClusterIdentifier=DB_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
        print ('Cluster will be deleted, you can monitore it on your redshift console.')
        
    except Exception as e:
        print(e)

    #04. Deleting role policy
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DB_CLUSTER_IDENTIFIER)['Clusters'][0]

    iam.detach_role_policy(RoleName=DB_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DB_IAM_ROLE_NAME)
    
if __name__ == '__main__':
    main()