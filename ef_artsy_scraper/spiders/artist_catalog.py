# artist_catalog.py

import scrapy
import json 
import string


class ArtistCatalog(scrapy.Spider):

	name = 'artist_catalog'

	def api_builder(self,letter=None,start_index=1):

		api_endpoint = "https://metaphysics-production.artsy.net/v2"
		
		headers = {
			'authority': 'metaphysics-production.artsy.net',
			'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="96", "Google Chrome";v="96"',
			'accept': '*/*',
			'x-timezone': 'America/New_York',
			'content-type': 'application/json',
			'sec-ch-ua-mobile': '?0',
			'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
			'sec-ch-ua-platform': '"macOS"',
			'origin': 'https://www.artsy.net',
			'sec-fetch-site': 'same-site',
			'sec-fetch-mode': 'cors',
			'sec-fetch-dest': 'empty',
			'referer': 'https://www.artsy.net/',
			'accept-language': 'en-US,en;q=0.9'
		}

		payload = json.dumps(
			{
				"id": "artistsRoutes_ArtistsByLetterQuery",
				"query": "query artistsRoutes_ArtistsByLetterQuery(\n  $letter: String!\n  $page: Int\n  $size: Int\n) {\n  viewer {\n    ...ArtistsByLetter_viewer_qU0ud\n  }\n}\n\nfragment ArtistsByLetter_viewer_qU0ud on Viewer {\n  artistsConnection(letter: $letter, page: $page, size: $size) {\n    pageInfo {\n      endCursor\n      hasNextPage\n    }\n    pageCursors {\n      ...Pagination_pageCursors\n    }\n    artists: edges {\n      artist: node {\n        internalID\n        name\n        href\n        id\n      }\n    }\n  }\n}\n\nfragment Pagination_pageCursors on PageCursors {\n  around {\n    cursor\n    page\n    isCurrent\n  }\n  first {\n    cursor\n    page\n    isCurrent\n  }\n  last {\n    cursor\n    page\n    isCurrent\n  }\n  previous {\n    cursor\n    page\n  }\n}\n",
				"variables": {
					"letter": letter,
					"page": start_index,
					"size": 100
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
				'letter': letter,
				'start_index':start_index
			}
		)

		return request

	def start_requests(self):

		for letter in string.ascii_lowercase:
			print(f'letter: {letter}')

			yield self.api_builder(letter)

	def parse(self,response):

		letter = response.meta['letter']
		start_index = response.meta['start_index']

		d = json.loads(response.text)
		page_data = d['data']['viewer']['artistsConnection']

		artist_list = page_data['artists']

		for artist in artist_list:
			yield artist['artist']

		has_next_page = page_data['pageInfo']['hasNextPage']


		if has_next_page:
			next_index = start_index + 1
			print(f'    letter_pagenum: {letter}/{next_index}')
			yield self.api_builder(letter,next_index)



