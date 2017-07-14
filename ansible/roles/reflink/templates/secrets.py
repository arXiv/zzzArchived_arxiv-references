import os

os.environ['AWS_ACCESS_KEY'] = "{{ aws_access_key }}"
os.environ['AWS_SECRET_KEY'] = "{{ aws_secret_key }}"
os.environ['REFLINK_AWS_ACCESS_KEY'] = "{{ aws_access_key }}"
os.environ['REFLINK_AWS_SECRET_KEY'] = "{{ aws_secret_key }}"
os.environ['REFLINK_AWS_REGION'] = "{{ aws_region }}"
os.environ['REFLINK_SQS_ENDPOINT'] = "sqs://{{ aws_access_key }}:{{ aws_secret_key }}@"
os.environ['REFLINK_S3_BUCKET'] = "{{ s3_bucket }}"
os.environ['APPLICATION_ROOT'] = '{{ application_root }}'
