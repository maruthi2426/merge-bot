import os
import boto3
from typing import Iterable

_S3 = None

def s3():
    global _S3
    if _S3 is None:
        _S3 = boto3.client('s3', region_name=os.environ.get('S3_REGION'))
    return _S3

def presign_get(bucket: str, key: str, expires: int = 3600) -> str:
    return s3().generate_presigned_url(
        ClientMethod='get_object',
        Params={'Bucket': bucket, 'Key': key},
        ExpiresIn=expires,
    )

def multipart_uploader(bucket: str, key: str, part_iter: Iterable[bytes], part_size: int = 5 * 1024 * 1024) -> str:
    s3c = s3()
    mp = s3c.create_multipart_upload(Bucket=bucket, Key=key, ContentType='video/x-matroska')
    upload_id = mp['UploadId']
    parts = []
    part_number = 1
    buffer = b''
    try:
        for chunk in part_iter:
            buffer += chunk
            if len(buffer) >= part_size:
                resp = s3c.upload_part(Bucket=bucket, Key=key, PartNumber=part_number, UploadId=upload_id, Body=buffer)
                parts.append({'ETag': resp['ETag'], 'PartNumber': part_number})
                part_number += 1
                buffer = b''
        if buffer:
            resp = s3c.upload_part(Bucket=bucket, Key=key, PartNumber=part_number, UploadId=upload_id, Body=buffer)
            parts.append({'ETag': resp['ETag'], 'PartNumber': part_number})
        s3c.complete_multipart_upload(Bucket=bucket, Key=key, MultipartUpload={'Parts': parts}, UploadId=upload_id)
    except Exception:
        s3c.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        raise
    return f's3://{bucket}/{key}'
