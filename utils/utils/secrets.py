import json
import logging
import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)


def get_sm_api_response(secret_name, region_name='us-east-1'):
    logger.info(f"Getting {secret_name} secrets.")

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name,
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print("The requested secret " + secret_name + " was not found")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            print("The request was invalid due to:", e)
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            print("The request had invalid params:", e)
        elif e.response['Error']['Code'] == 'DecryptionFailure':
            print("The requested secret can't be decrypted using the provided KMS key:", e)
        elif e.response['Error']['Code'] == 'InternalServiceError':
            print("An error occurred on service side:", e)
        raise e
    except:
        raise

    return get_secret_value_response


def get_secret(secret_name, region_name='us-east-1', string_format='JSON'):
    api_response = get_sm_api_response(secret_name, region_name='us-east-1')
    if string_format == 'JSON':
        return json.loads(api_response['SecretString'])
    elif string_format == 'PLAIN TEXT':
        return api_response['SecretString']