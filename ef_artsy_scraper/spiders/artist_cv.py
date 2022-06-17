# artist_cv.py

import scrapy
import json 
import pandas as pd

# for accessing files on AWS S3
import os
import boto3


class ArtistCV(scrapy.Spider):

	name='artist_cv'

	show_type_configs = {
		
		'solo' : {
			'atAFair' : False,
			'soloShow' : True
		},
		'fair' : {
			'atAFair' : True,
			'soloShow' : False
		},
		'group' : {
			'atAFair' : False,
			'soloShow' : False
		}
	}

	def api_builder(self,artist_slug,show_type,cursor=''):

		api_endpoint = "https://metaphysics-production.artsy.net/v2"

		show_params = self.show_type_configs[show_type]

		headers = {
			'authority': 'metaphysics-production.artsy.net',
			'accept': '*/*',
			'accept-language': 'en-US,en;q=0.9',
			'content-type': 'application/json',
			'origin': 'https://www.artsy.net',
			'referer': 'https://www.artsy.net/',
			'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
			'sec-ch-ua-mobile': '?0',
			'sec-ch-ua-platform': '"macOS"',
			'sec-fetch-dest': 'empty',
			'sec-fetch-mode': 'cors',
			'sec-fetch-site': 'same-site',
			'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.41 Safari/537.36',
			'x-access-token': 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiI2MTk1OTVmMjliYjQ3NjAwMGI3NDNlZTQiLCJzYWx0X2hhc2giOiI1ZDVjY2Q1ZmQzNWI4NTFlNmFhOGNlMjNiNDliNDI2MSIsInJvbGVzIjoidXNlciIsInBhcnRuZXJfaWRzIjpbXSwib3RwIjpmYWxzZSwiZXhwIjoxNjUzOTQ4MDk2LCJpYXQiOjE2NDg3NjQwOTYsImF1ZCI6IjVkNDA5OTZlNmU2MDQ5MDAwNzQ5MGZhMiIsImlzcyI6IkdyYXZpdHkiLCJqdGkiOiI2MjQ2MjRjMGFjMTRjMDAwMGNkNTU2ZGQifQ.wE20OgmPgUoZF4b040wGQLoPlff2ATfF0q0SUfkQRpM',
			'x-timezone': 'America/New_York',
			'x-user-id': '619595f29bb476000b743ee4'
		}

		payload = json.dumps(
			{
				"id": "ArtistCVGroupQuery",
				"query": "query ArtistCVGroupQuery(\n  $count: Int\n  $cursor: String\n  $slug: String!\n  $sort: ShowSorts\n  $atAFair: Boolean\n  $soloShow: Boolean\n  $isReference: Boolean\n  $visibleToPublic: Boolean\n) {\n  artist(id: $slug) {\n    ...ArtistCVGroup_artist_4A66pF\n    id\n  }\n}\n\nfragment ArtistCVGroup_artist_4A66pF on Artist {\n  slug\n  showsConnection(first: $count, after: $cursor, sort: $sort, atAFair: $atAFair, soloShow: $soloShow, isReference: $isReference, visibleToPublic: $visibleToPublic) {\n    pageInfo {\n      hasNextPage\n      endCursor\n    }\n    edges {\n      node {\n        id\n        partner {\n          __typename\n          ... on ExternalPartner {\n            name\n            id\n          }\n          ... on Partner {\n            name\n            href\n          }\n          ... on Node {\n            __isNode: __typename\n            id\n          }\n        }\n        name\n        startAt(format: \"YYYY\")\n        city\n        href\n        __typename\n      }\n      cursor\n    }\n  }\n}\n",
				"variables": {
					"count": 1000,
					"cursor": cursor,
					"slug": artist_slug,
					"sort": "START_AT_DESC",
					"atAFair": show_params['atAFair'],
					"soloShow": show_params['soloShow'],
					"isReference": True,
					"visibleToPublic": False
				}
			}
		)

		request = scrapy.Request(
			url=api_endpoint,
			callback=self.parse,
			method='POST',
			headers=headers,
			body=payload,
			meta={
				'artist_slug': artist_slug,
				'show_type':show_type,
			}
		)

		return request


	def start_requests(self):

		# connect to aws s3

		AWS_ACCESS_KEY_ID = self.settings['AWS_ACCESS_KEY_ID']
		AWS_SECRET_ACCESS_KEY = self.settings['AWS_SECRET_ACCESS_KEY']

		s3 = boto3.client(
		    's3',
		    aws_access_key_id=AWS_ACCESS_KEY_ID,
		    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
		)

		bucket = 'euclidsfund-data-pipeline'
		prefix = 'data_acquisition/artsy/artist_catalog/preprocessed/'

		# identify the latest export of preprocessed artist slugs
		list_of_files = []
		results = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
		for obj in results['Contents']:
		    
		    res_key = obj['Key']
		    if res_key != prefix:
		        list_of_files.append(res_key)
		        
		latest_fp = list_of_files[-1]

		# pull down data from identified (bucket,key) pair
		s3_file_contents = s3.get_object(Bucket=bucket, Key=latest_fp) 
		df = pd.read_csv(s3_file_contents['Body'])

		artist_list = list(df.artist_slug.values)

		for artist_slug in artist_list:

			for show_type in self.show_type_configs.keys():

				yield self.api_builder(
					artist_slug,
					show_type
					)

	def prefix_dict(self,target_dict,prefix):
		"""add prefix to keys in dict"""
		temp_dict = {}

		keys = target_dict.keys()
		for old_key in keys:
			new_key = f'{prefix}_{old_key}'
			new_key = new_key.replace('__','_')
			temp_dict[new_key] = target_dict[old_key]

		return temp_dict

	def parse(self,response):
		
		artist_slug = response.meta['artist_slug']
		show_type = response.meta['show_type']

		d = json.loads(response.text)
		page_data = d['data']['artist']['showsConnection']

		shows = page_data['edges']

		for show in shows:

			show_dict = show['node']
			partner_dict = show_dict.pop('partner')

			show_dict = self.prefix_dict(show_dict,'show')
			partner_dict = self.prefix_dict(partner_dict,'partner')

			full_dict = {**show_dict,**partner_dict}

			full_dict['artist_slug'] = artist_slug
			full_dict['show_type'] = show_type

			yield full_dict

			# Check to see if there are more results to grab
			has_next_page = page_data['pageInfo']['hasNextPage']

			if has_next_page:
				
				cursor = page_data['pageInfo']['endCursor']
				
				yield self.api_builder(
					artist_slug=artist_slug,
					show_type=show_type,
					cursor = cursor
				)






