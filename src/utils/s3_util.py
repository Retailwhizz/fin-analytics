import os
import time
import urllib.parse

import boto3
import botocore
from botocore.exceptions import NoCredentialsError, ClientError

from src import config
from src.common.constant import Constant
from src.config import Config
from src.utils.logger import logger

ACCESS_KEY = config.AWS_ACCESS_KEY
SECRET_KEY = config.ASW_SECRET_ACCESS_KEY
FILE_DIRECTORY = config.AWS_FILE_DIRECTORY
HOST = config.AWS_S3_HOST


def upload_to_s3(df, bucket, file_type, file_name):
    """Upload a template to an S3 bucket
        :param df: file data frame
        :param bucket: Bucket to upload to
        :param file_type: file type
        :param file_name: file name
        :return: s3key if file is uploaded else None
        """
    # Creating Session With Boto3.
    session = boto3.Session(
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    # Creating S3 Resource From the Session.
    s3 = session.resource('s3')
    fileName = file_name
    outputFile = open(fileName, "wb")
    object_name = FILE_DIRECTORY + fileName
    if file_type != 'csv':
        # df = pd.read_excel(file, skiprows=skip_rows)
        df.to_excel(outputFile, index=False, engine='xlsxwriter')
    else:
        # df = pd.read_csv(file, skiprows=skip_rows, delim_whitespace=True)
        df.to_csv(outputFile, index=False)
    outputFile.close()
    try:
        result = s3.Bucket(bucket).upload_file(outputFile.name, object_name)
    except ClientError as e:
        logger.error(e)
        os.remove(outputFile.name)
        return None
    os.remove(outputFile.name)
    return bucket + "/" + object_name


def downloadS3File(s3_key, account=Constant.DEFAULT_ACCOUNT):
    s3 = get_s3_resource(account)
    token_array = s3_key.split("/")
    size = len(token_array)
    bucket = token_array[0]
    file_name = token_array[size - 1]
    object_name = "/".join(token_array[1:])
    try:
        s3.Bucket(bucket).download_file(object_name, file_name)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            logger.error("The object does not exist.")
    return file_name


def get_s3_resource(account=Constant.DEFAULT_ACCOUNT, requireClient=False):
    if account == Constant.SCM_ACCOUNT:
        return get_cross_functional_s3_resource(Config.SCM_ONE_ROOF_CROSS_FUNCTIONAL_ROLE,
                                                Config.SCM_ONE_ROOF_SESSION_NAME)
    elif account == Constant.MERCURY_ACCOUNT:
        return get_cross_functional_s3_resource(Config.MERCURY_CROSS_FUNCTIONAL_ROLE,
                                                Config.MERCURY_SESSION_NAME, requireClient)
    else:
        # Creating Session With Boto3.
        session = boto3.Session(
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY
        )
        # Creating S3 Resource From the Session.
        if requireClient:
            return session.client('s3')

        return session.resource('s3')


def get_cross_functional_s3_resource(role_id, session_name, requireClient=False):
    sts_client = boto3.client('sts')
    assumed_role_object = sts_client.assume_role(
        RoleArn=role_id,
        RoleSessionName=session_name
    )
    credentials = assumed_role_object['Credentials']

    if requireClient:
        return boto3.client(
            's3',
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken'],
        )

    return boto3.resource(
        's3',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken'],
    )


def upload_response_file_to_s3(file, bucket):
    session = boto3.Session(
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    # Creating S3 Resource From the Session.
    object_name = FILE_DIRECTORY + file
    s3 = session.resource('s3')
    try:
        result = s3.Bucket(bucket).upload_file(file, object_name)
    except ClientError as e:
        logger.error(e)
        os.remove(file.name)
        return None
    os.remove(file)
    return bucket + "/" + object_name


def upload_file_to_s3(file, bucket, path):
    # Creating Session With Boto3.
    key = path + str(round(time.time() * 1000)) + "_" + file.filename
    session = boto3.Session(
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    s3 = session.resource('s3')
    try:
        s3Object = s3.Object(bucket, key)
        s3Object.put(Body=file)
        response = {'s3Key': key, 'bucket': bucket,
                    's3Url': "https://" + bucket + ".s3.ap-south-1.amazonaws.com/" + key}
        return response
    except ClientError as e:
        logger.error(e)
        return None


def upload_file_to_s3_generic(file, s3_bucket, s3_key, account=Constant.DEFAULT_ACCOUNT):
    s3 = get_s3_resource(account)
    try:
        s3.Bucket(s3_bucket).upload_file(file, s3_key)
    except ClientError as e:
        logger.error(e)
        os.remove(file.name)
        return None
    return HOST + "/" + s3_bucket + "/" + urllib.parse.quote(s3_key)


def generate_pre_signed_url_generic(s3_bucket, s3_key, expiration_time, account=Constant.DEFAULT_ACCOUNT):
    requireClient = True #S3.Client is required incase of generating preSignedUrl
    s3 = get_s3_resource(account, requireClient)
    try:
        result = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': s3_bucket, 'Key': s3_key},
            ExpiresIn=expiration_time)
    except ClientError as e:
        logger.error(e)
        return None
    return result
