import json
import sys

import boto3


def read_config(cfg_path):
	with open(cfg_path, 'r') as f:
		return json.load(f)


def make_s3_client(cfg):
	return boto3.client(
		service_name='s3',
		region_name=cfg['region_name'],
		aws_access_key_id=cfg['aws_access_key_id'],
		aws_secret_access_key=cfg['aws_secret_access_key']
	)


def main(argv):
	bucket_name = argv[1]
	s3_file_names = argv[2]

	s3_cfg = read_config('s3-config.json')

	s3_client = make_s3_client(s3_cfg)

	for file in s3_file_names.split(','):
		s3_client.download_file(bucket_name, file, './' + file.split('/')[-1])


if __name__ == '__main__':
	main(sys.argv)
