#!/usr/bin/env python3

"""This script automatically fetches and updates data feeds for a Skrunk server instance."""

import json
import re
import time
from datetime import datetime, timezone
from typing import Generator, TypedDict
from urllib.parse import urlparse

import praw

from skrunk_api import Session, SessionError

## The number of seconds to wait before fetching new documents
FETCH_DELAY = 3600


class Feed(TypedDict, total=True):
    """
    Represents a feed with metadata and configuration.

    Attributes:
        id (str): Unique identifier for the feed.
        name (str): Name of the feed.
        creator (str): Identifier or name of the feed creator.
        created (datetime): Timestamp when the feed was created.
        inactive (bool): Whether the feed is inactive.
        kind (str): Type or category of the feed.
        url (str): URL associated with the feed.
        notify (bool): Whether notifications are enabled for this feed.
        origin (str | None): Optional origin information for the feed.
    """
    id: str
    name: str
    creator: str
    created: datetime
    inactive: bool
    kind: str
    url: str
    notify: bool
    origin: str | None


class Document(TypedDict, total=True):
    """
    TypedDict representing a document in the feed system.

    Attributes:
        id (str): Unique identifier for the document.
        feed (str): Name or identifier of the feed to which the document belongs.
        author (str | None): Author of the document, or None if not specified.
        posted (datetime | None): Date and time when the document was posted, or None if not posted.
        body (str): Raw text content of the document.
        body_html (str): HTML-rendered content of the document.
        created (datetime): Date and time when the document was created.
        updated (datetime | None): Date and time when the document was last updated,
            or None if never updated.
    """
    id: str
    feed: str
    author: str | None
    posted: datetime | None
    body: str
    body_html: str
    created: datetime
    updated: datetime | None


class API:
    """
    API class that encapsulates a various API clients.

    Attributes:
        reddit (praw.Reddit): An instance of the PRAW Reddit client 
            used to interact with the Reddit API.
    """
    reddit: praw.Reddit


def log(msg: str, end: str | None = None) -> None:
    """
    Logs a message to the console with optional end character.

    Args:
        msg (str): The message to log.
        end (str | None, optional): String appended after the message. Defaults to None.

    Returns:
        None
    """
    print(msg, flush=True, end=end)


def get_feeds(api: Session) -> Generator[Feed, None, None]:
    """
    Retrieves active feeds from the given API session in batches.

    Args:
        api (Session): An API session object used to make calls to the feed service.

    Yields:
        Feed: An active feed object retrieved from the API.

    Raises:
        SessionError: If an error occurs during API calls, it is logged and the function continues.
    """
    try:
        total = api.call('countFeeds')
    except SessionError as e:
        log(f'SKRUNK: {e}')
        total = 0

    feed_batch_size = 20
    for i in range(0, total, feed_batch_size):
        try:
            feed_list: list[Feed] = api.call('getFeeds', {
                'start': i,
                'count': feed_batch_size,
            })

            yield from (i for i in feed_list if not i['inactive'])
        except SessionError as e:
            log(f'SKRUNK: {e}')


def fetch_next_document(feed: Feed, api: Session) -> None:
    """
    Fetches the next document for a given feed and updates or creates a feed document accordingly.

    If a new document is created and notifications are enabled,
    it sends a notification to the feed creator.

    Any errors encountered during the process are logged.

    Args:
        feed (Feed): The feed dictionary containing feed metadata and configuration.
        api (Session): The API session object used to interact with the backend and external APIs.

    Returns:
        None
    """

    if feed['kind'] not in ['markdown_recursive']:
        log(f'ERROR: Invalid feed kind "{feed["kind"]}" in feed {feed["id"]}!')
        return

    # Try to determine what website the feed URL links to so we can use the correct API
    hostname = urlparse(feed['url']).hostname
    if hostname is None:
        log(f'ERROR: Feed {feed["id"]} has invalid URL "{feed["url"]}"')
        return

    addr = hostname.split('.')
    if len(addr) < 1:
        log(f'ERROR: Feed {feed["id"]} has invalid hostname "{hostname}"')
        return

    if addr[-2::] == ['reddit', 'com']:
        # We know API is reddit.
        feed['origin'] = 'reddit'
    else:
        log(f'ERROR: Could not determine origin for feed {feed["id"]} hostname "{hostname}"')
        return

    next_url = feed['url']
    document_body = None
    document_id = None

    # Assume feed kind is valid, we already validated it.
    if feed['kind'] == 'markdown_recursive':
        # Get most recent feed document, and search for (next)[...] to get the next document.
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

            # Search for (next)[...]
            m = re.search(r'\[[Nn]ext[^\w\]]*\]\(([^)]*)\)', doc['body'])
            if m is not None:
                # If pattern was found, fetch the next URL
                next_url = m.group(1)
            else:
                document_body = doc['body']
                document_id = doc['id']

    # At this point, we have a next_url to fetch,
    # and we can create the document.
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
        post = API.reddit.submission(url=next_url)
        log('Fetched post data.')

        document['title'] = post.title
        document['body'] = post.selftext
        document['author'] = post.author.name
        document['posted'] = datetime.fromtimestamp(
            post.created_utc,
            timezone.utc
        ).strftime('%Y-%m-%d %H:%M:%S')
    else:
        log(f'ERROR: Cannot determine origin of feed {feed["id"]}')
        return

    try:
        if document_id:
            # We're just updating an existing document

            if document_body == document['body']:
                # Body hasn't changed, so do nothing.
                return

            result = api.call('updateFeedDocument', {
                'id': document_id,
                'body': document['body'],
            })
        else:
            # We're creating a new document
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
    """
    Main entry point for the skrunk-feeds application.

    Returns:
        None
    """

    log('Initializing... ', end='')

    with open('config.json', 'r', encoding='utf-8') as fp:
        config = json.load(fp)

    # Start skrunk session
    api = Session(config['skrunk']['api_key'], config['skrunk']['url'])

    # Start reddit session
    API.reddit = praw.Reddit(config['reddit']['username'], check_for_async=False)

    log('Done.')
    while True:
        for feed in get_feeds(api):
            # pylint: disable=broad-except
            try:
                fetch_next_document(feed, api)
            except Exception as e:
                # We don't want one feed failure to kill the whole process.
                # Log it and move on.
                log(f'EXCEPTION: {e}')
            # pylint: enable=broad-except

        time.sleep(FETCH_DELAY)


if __name__ == '__main__':
    main()
