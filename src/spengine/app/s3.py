import boto3


class S3:
    def __init__(self, bucket_name: str, client: boto3.client, content_type: str = "application/octet-stream"):
        self.bucket = bucket_name
        self.client = client
        self.content_type = content_type

    def put_object(self, filename, data: bytes):
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=filename,
                Body=data,
                ContentType=self.content_type,
            )
        except Exception as e:
            raise e

    def read_object(self, filename):
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=filename)
            return response["Body"].iter_chunks()
        except Exception as e:
            raise e
