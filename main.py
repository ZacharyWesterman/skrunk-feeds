#!/usr/bin/env python3
from skrunk_api import Session, SessionError
import json, re
from typing import Generator, TypedDict
from datetime import datetime, timezone
from urllib.parse import urlparse
import time

FETCH_DELAY = 3600 #How many seconds to wait before fetching new documents

#APIs
import praw

class Feed(TypedDict, total=True):
	id: str
	name: str
	creator: str
	created: datetime
	kind: str
	url: str
	notify: bool

class Document(TypedDict, total=True):
	id: str
	feed: str
	author: str|None
	posted: datetime|None
	body: str
	body_html: str
	created: datetime
	updated: datetime|None

class API:
	reddit: praw.Reddit

def log(msg: str, end=None) -> None:
	print(msg, flush=True, end=end)

def get_feeds(api: Session) -> Generator[Feed, None, None]:
	try:
		total = api.call('countFeeds')
	except SessionError as e:
		log(f'SKRUNK: {e}')
		total = 0

	feed_batch_size = 20
	for i in range(0, total, feed_batch_size):
		try:
			feed_list = api.call('getFeeds', {
				'start': i,
				'count': feed_batch_size,
			})

			for feed in [ i for i in feed_list if not i['inactive'] ]:
				yield feed
		except SessionError as e:
			log(f'SKRUNK: {e}')

def fetch_next_document(feed: Feed, api: Session) -> Generator[Document, None, None]:
	if feed['kind'] not in ['markdown_recursive']:
		log(f'ERROR: Invalid feed kind "{feed["kind"]}" in feed {feed["id"]}!')
		return

	#Try to determine what website the feed URL links to so we can use the correct API
	hostname = urlparse(feed['url']).hostname
	addr = hostname.split('.')
	if len(addr) < 1:
		log(f'ERROR: Feed {feed["id"]} has invalid hostname "{hostname}"')
		return

	if addr[-2::] == ['reddit', 'com']:
		#We know API is reddit.
		feed['origin'] = 'reddit'
	else:
		log(f'ERROR: Could not determine origin for feed {feed["id"]} hostname "{hostname}"')
		return

	next_url = feed['url']
	document_body = None
	document_id = None

	#Assume feed kind is valid, we already validated it.
	if feed['kind'] == 'markdown_recursive':
		#Get most recent feed document, and search for (next)[...] to get the next document.
		try:
			documents = api.call('getFeedDocuments', {
				'feed': feed['id'],
				'start': 0,
				'count': 1,
				'sorting': {
					'fields': ['created'],
					'descending': True,
				},
			})
		except SessionError as e:
			log(f'SKRUNK: {e}')
			return

		if len(documents):
			doc = documents[0]
			next_url = doc['url']

			#Search for (next)[...]
			m = re.search(r'\[[Nn]ext\]\(([^)]*)\)', doc['body'])
			if m is not None:
				#If pattern was found, fetch the next URL
				next_url = m.group(1)
			else:
				document_body = doc['body']
				document_id = doc['id']

	#At this point, we have a next_url to fetch,
	#and we can create the document.
	document = {
		'feed': feed['id'],
		'author': None,
		'posted': None,
		'body': '[ERROR: NO BODY]',
		'title': None,
		'url': next_url,
	}

	if feed['origin'] == 'reddit':
		log('Reaching out to Reddit API... ', end='')
		post = API.reddit.submission(url = next_url)
		log('Fetched post data.')

		document['title'] = post.title
		document['body'] = post.selftext
		document['author'] = post.author.name
		document['posted'] = datetime.fromtimestamp(post.created_utc, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
	else:
		log(f'ERROR: Cannot determine origin of feed {feed["id"]}')
		return

	try:
		if document_id:
			#We're just updating an existing document

			if document_body == document['body']:
				#Body hasn't changed, so do nothing.
				return

			result = api.call('updateFeedDocument', {
				'id': document_id,
				'body': document['body'],
			})
		else:
			#We're creating a new document
			result = api.call('createFeedDocument', document)

			if feed['notify']:
				api.call('sendNotification', {
					'username': feed['creator'],
					'title': feed['name'],
					'body': 'A new post has been added to your feed.',
					'category': 'feed',
				})
	except SessionError as e:
		log(f'SKRUNK: {e}')
		return

	if result['__typename'] != 'FeedDocument':
		log(f'SKRUNK: {result["message"]}')
		return

	log(f'{"Updated" if document_id else "Fetched new"} document for feed {feed["id"]}')

def main() -> None:
	log('Initializing... ', end='')

	with open('config.json', 'r') as fp:
		config = json.load(fp)

	#Start skrunk session
	api = Session(config['skrunk']['api_key'], config['skrunk']['url'])

	#Start reddit session
	API.reddit = praw.Reddit(config['reddit']['username'], check_for_async = False)

	log('Done.')
	while True:
		for feed in get_feeds(api):
			try:
				fetch_next_document(feed, api)
			except Exception as e:
				log(f'EXCEPTION: {e}')

		time.sleep(FETCH_DELAY)

if __name__ == '__main__':
	main()
