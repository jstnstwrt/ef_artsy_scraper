# artist_pdp_top_level.py

import scrapy
import json 
import pandas as pd

# for accessing files on AWS S3
from dotenv import load_dotenv
import os
import boto3


class ProfileBasics(scrapy.Spider):

	name = 'profile_basics'

	def api_builder(self,artist_slug=None):

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

		payload = json.dumps({
		  "id": "artistRoutes_TopLevelQuery",
		  "query": "query artistRoutes_TopLevelQuery(\n  $artistID: String!\n) {\n  artist(id: $artistID) @principalField {\n    ...ArtistApp_artist\n    slug\n    statuses {\n      shows\n      cv(minShowCount: 0)\n      articles\n    }\n    counts {\n      forSaleArtworks\n      auctionResults\n    }\n    related {\n      genes {\n        edges {\n          node {\n            slug\n            id\n          }\n        }\n      }\n    }\n    highlights {\n      artistPartnersConnection: partnersConnection(first: 10, displayOnPartnerProfile: true, representedBy: true, partnerCategory: [\"blue-chip\", \"top-established\", \"top-emerging\"]) {\n        edges {\n          node {\n            categories {\n              slug\n              id\n            }\n            id\n          }\n          id\n        }\n      }\n    }\n    insights {\n      type\n    }\n    biographyBlurb(format: HTML, partnerBio: false) {\n      text\n    }\n    id\n  }\n}\n\nfragment ArtistApp_artist on Artist {\n  slug\n  statuses {\n    shows\n    cv(minShowCount: 0)\n    articles\n    artworks\n    auctionLots\n  }\n  counts {\n    forSaleArtworks\n    auctionResults\n  }\n  related {\n    genes {\n      edges {\n        node {\n          slug\n          id\n        }\n      }\n    }\n  }\n  highlights {\n    artistPartnersConnection: partnersConnection(first: 10, displayOnPartnerProfile: true, representedBy: true, partnerCategory: [\"blue-chip\", \"top-established\", \"top-emerging\"]) {\n      edges {\n        node {\n          categories {\n            slug\n            id\n          }\n          id\n        }\n        id\n      }\n    }\n  }\n  insights {\n    type\n  }\n  biographyBlurb(format: HTML, partnerBio: false) {\n    text\n  }\n  ...ArtistMeta_artist\n  ...ArtistHeader_artist\n  ...BackLink_artist\n  internalID\n  name\n}\n\nfragment ArtistHeader_artist on Artist {\n  ...FollowArtistButton_artist\n  ...SelectedCareerAchievements_artist\n  artistHighlights: highlights {\n    partnersConnection(first: 10, displayOnPartnerProfile: true, representedBy: true, partnerCategory: [\"blue-chip\", \"top-established\", \"top-emerging\"]) {\n      edges {\n        node {\n          categories {\n            slug\n            id\n          }\n          id\n        }\n        id\n      }\n    }\n  }\n  auctionResultsConnection(recordsTrusted: true, first: 1, sort: PRICE_AND_DATE_DESC) {\n    edges {\n      node {\n        price_realized: priceRealized {\n          display(format: \"0.0a\")\n        }\n        organization\n        sale_date: saleDate(format: \"YYYY\")\n        id\n      }\n    }\n  }\n  image {\n    cropped(width: 200, height: 200) {\n      src\n      srcSet\n    }\n  }\n  internalID\n  slug\n  name\n  formattedNationalityAndBirthday\n  counts {\n    follows\n    forSaleArtworks\n  }\n  biographyBlurb(format: HTML, partnerBio: false) {\n    credit\n    partnerID\n    text\n  }\n}\n\nfragment ArtistMetaCanonicalLink_artist on Artist {\n  slug\n  statuses {\n    shows\n    cv(minShowCount: 0)\n    articles\n    auctionLots\n    artworks\n  }\n  highlights {\n    partnersConnection(first: 10, displayOnPartnerProfile: true, representedBy: true, partnerCategory: [\"blue-chip\", \"top-established\", \"top-emerging\"]) {\n      edges {\n        __typename\n        id\n      }\n    }\n  }\n  biographyBlurb(format: HTML, partnerBio: false) {\n    text\n  }\n  related {\n    genes {\n      edges {\n        node {\n          __typename\n          id\n        }\n      }\n    }\n  }\n  insights {\n    __typename\n  }\n}\n\nfragment ArtistMeta_artist on Artist {\n  slug\n  name\n  nationality\n  birthday\n  deathday\n  gender\n  href\n  meta {\n    description\n  }\n  alternate_names: alternateNames\n  image {\n    versions\n    large: url(version: \"large\")\n    square: url(version: \"square\")\n  }\n  counts {\n    artworks\n  }\n  blurb\n  artworks_connection: artworksConnection(first: 10, filter: IS_FOR_SALE, published: true) {\n    edges {\n      node {\n        title\n        date\n        description\n        category\n        price_currency: priceCurrency\n        listPrice {\n          __typename\n          ... on PriceRange {\n            minPrice {\n              major\n              currencyCode\n            }\n            maxPrice {\n              major\n            }\n          }\n          ... on Money {\n            major\n            currencyCode\n          }\n        }\n        availability\n        href\n        image {\n          small: url(version: \"small\")\n          large: url(version: \"large\")\n        }\n        partner {\n          name\n          href\n          profile {\n            image {\n              small: url(version: \"small\")\n              large: url(version: \"large\")\n            }\n            id\n          }\n          id\n        }\n        id\n      }\n    }\n  }\n  ...ArtistMetaCanonicalLink_artist\n}\n\nfragment BackLink_artist on Artist {\n  name\n  slug\n}\n\nfragment FollowArtistButton_artist on Artist {\n  id\n  internalID\n  name\n  slug\n  is_followed: isFollowed\n  counts {\n    follows\n  }\n}\n\nfragment SelectedCareerAchievements_artist on Artist {\n  highlights {\n    partnersConnection(first: 10, displayOnPartnerProfile: true, representedBy: true, partnerCategory: [\"blue-chip\", \"top-established\", \"top-emerging\"]) {\n      edges {\n        node {\n          categories {\n            slug\n            id\n          }\n          id\n        }\n        id\n      }\n    }\n  }\n  insights {\n    type\n    label\n    entities\n  }\n  auctionResultsConnection(recordsTrusted: true, first: 1, sort: PRICE_AND_DATE_DESC) {\n    edges {\n      node {\n        price_realized: priceRealized {\n          display(format: \"0.0a\")\n        }\n        organization\n        sale_date: saleDate(format: \"YYYY\")\n        id\n      }\n    }\n  }\n  slug\n}\n",
		  "variables": {
		    "artistID": artist_slug
		  }
		})

		request = scrapy.Request(
			url=api_endpoint,
			callback=self.parse,
			method='POST',
			headers=headers,
			body=payload,
			meta={
				'artist_slug':artist_slug
			}
		)

		return request

	def start_requests(self):

		connect to aws s3
		load_dotenv()

		AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
		AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

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

		for artist_slug in artist_list[:5]:

			yield self.api_builder(artist_slug)

	def parse(self,response):

		d = json.loads(response.text)
		page_data = d['data']['artist']

		yield page_data

