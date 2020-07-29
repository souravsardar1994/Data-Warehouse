import boto3
import configparser
import json

def main():
    """
    creates the redshift cluster using parameters on .cfg
    """

    #01. Importing AWS parameters
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY = config.get('AWS','KEY')
    SECRET = config.get('AWS','SECRET')

    DB_CLUSTER_TYPE = config.get("CLUSTER","DB_CLUSTER_TYPE")
    DB_NUM_NODES = config.get("CLUSTER","DB_NUM_NODES")
    DB_NODE_TYPE = config.get("CLUSTER","DB_NODE_TYPE")

    DB_CLUSTER_IDENTIFIER = config.get("CLUSTER","DB_CLUSTER_IDENTIFIER")
    DB_NAME = config.get("CLUSTER","DB_NAME")
    DB_USER = config.get("CLUSTER","DB_USER")
    DB_PASSWORD = config.get("CLUSTER","DB_PASSWORD")
    DB_PORT = config.get("CLUSTER","DB_PORT")

    DB_IAM_ROLE_NAME = config.get("CLUSTER", "DB_IAM_ROLE_NAME")
    
    print("Creating clients for AWS Services")

    #02. Creating clients for AWS Services
    ec2 = boto3.resource (
        'ec2',
        region_name='us-west-2',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )
    
    s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )

    iam = boto3.client (
        'iam',
        region_name='us-west-2',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )

    redshift = boto3.client (
        'redshift',
        region_name='us-west-2',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )

    #03. Creating IAM role
    try:
        sparkifyRole = iam.create_role (
            Path='/',
            RoleName=DB_IAM_ROLE_NAME,
            Description='Allows Redshift clusters to call AWS Services on your behalf.',
            AssumeRolePolicyDocument=json.dumps ({
                'Statement': [{
                    'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {'Service': 'redshift.amazonaws.com'}
                }],
                'Version': '2012-10-17'
            })
        )

    except Exception as e:
        print(e)

    #04. Ataching policy for IAM role
    iam.attach_role_policy (
        RoleName=DB_IAM_ROLE_NAME,
        PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
    )['ResponseMetadata']['HTTPStatusCode']
    
    roleArn = iam.get_role(RoleName=DB_IAM_ROLE_NAME)['Role']['Arn']
    print(roleArn)

    #05. Creating redshift cluster
    try:
        print('Creating cluster...')
        response = redshift.create_cluster(        

            #Hardware parameters
            ClusterType=DB_CLUSTER_TYPE,
            NodeType=DB_NODE_TYPE,
            NumberOfNodes=int(DB_NUM_NODES),

            #Identifiers & credentials parameters
            DBName=DB_NAME,
            ClusterIdentifier=DB_CLUSTER_IDENTIFIER,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,        

            #Role parameters
            IamRoles=[roleArn]
        )
        print ('Cluster will be created on  redshift console.')
        
    except Exception as e:
        print(e)
        
    #06. Open an incoming TCP port to access the cluster endpoint
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DB_CLUSTER_IDENTIFIER)['Clusters'][0]

    try:
        print('Opening an incoming TCP port to access the cluster endpoint...')
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DB_PORT),
            ToPort=int(DB_PORT)
        )
        print('TCP port opened.')
    except Exception as e:
        print(e)
        
if __name__ == '__main__':
    main()