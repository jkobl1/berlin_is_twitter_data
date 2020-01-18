#!/usr/bin/env python3
import itertools
import os

from everypolitician import EveryPolitician
import requests
import scraperwiki


ep = EveryPolitician()

consumer_key = os.environ.get('MORPH_TWITTER_CONSUMER_KEY')
consumer_secret = os.environ.get('MORPH_TWITTER_CONSUMER_SECRET')

# get a bearer token from twitter
def _get_token(consumer_key, consumer_secret):
    auth = requests.auth.HTTPBasicAuth(consumer_key, consumer_secret)
    auth_url = 'https://api.twitter.com/oauth2/token'
    auth_data = {'grant_type': 'client_credentials'}
    r = requests.post(url=auth_url, data=auth_data, auth=auth)
    j = r.json()
    return j['access_token']

# Run twitter API query
def _run_query(payload):
    r = requests.post(
        'https://api.twitter.com/1.1/users/lookup.json',
        data=payload,
        headers=auth_header,
    )
    data = r.json()
    if r.status_code != 200:
        if data.get('errors'):
            for error in data['errors']:
                print("Error: {msg} ({code})".format(
                    msg=error['message'],
                    code=error['code'])
                )
        else:
            print("Error: Some unknown problem")
        return None
    return data

# auth stuff
if not consumer_key:
    raise Exception("Please set env variable: MORPH_TWITTER_CONSUMER_KEY")
if not consumer_secret:
    raise Exception("Please set env variable: MORPH_TWITTER_CONSUMER_SECRET")
token = _get_token(consumer_key, consumer_secret)
auth_header = {'Authorization': 'Bearer {token}'.format(token=token)}

ep = EveryPolitician()

ep_twitter_data = []
# get the routes to all the popolo files
for country in ep.countries():
    print('Fetching EP data for {country_name} ...'.format(country_name=country.name))
    for legislature in country.legislatures():
        # build a list of all the Twitter handles & IDs on EveryPolitician
        for person in legislature.popolo().persons:
            twitter_handles = person.twitter_all
            twitter_ids = person.identifier_values('twitter')
            # TODO this assumes identifier ordering
            # and contact detail ordering is the same! :\
            for handle, id_ in itertools.zip_longest(twitter_handles, twitter_ids):
                ep_twitter_data.append({
                    'person_id': person.id,
                    'handle': handle,
                    'twitter_id': id_,
                })

updates = []

# 1. If we have IDs, we want to check handles
ep_data_with_ids = {v['twitter_id']: v for v in ep_twitter_data if v['twitter_id']}
ids_to_check = list(ep_data_with_ids.keys())
api_response_data = {}
for lower in range(0, len(ids_to_check), 100):
    print('Fetching twitter data: {lower:,} to {upper:,} of {len:,} IDs ...'.format(
        lower=lower + 1, upper=lower + 100, len=len(ids_to_check)
    ))
    user_ids = ','.join(ids_to_check[lower:lower + 100])
    payload = {'user_id': user_ids}
    api_response_data_partial = _run_query(payload)
    if not api_response_data_partial:
        continue
    api_response_data.update({x['id']: x for x in api_response_data_partial})

for x in ep_data_with_ids.values():
    if x['twitter_id'] not in api_response_data:
        # Twitter ID not found - this account may have been
        # deleted or suspended
        #
        # TODO this assumes the stored ID matches the stored handle,
        # but this is not necessarily the case (the handle may have
        # subsequently been updated.) After the ID lookup fails,
        # we should really do an API lookup on the handle.
        print('{person_id}: Twitter ID {id_} (@{handle}) not found.'.format(
            person_id=x['person_id'],
            id_=x['twitter_id'],
            handle=x['handle'],
        ))
        updates.append({
            'id': x['person_id'],
            'twitter_id': x['twitter_id'],
            'twitter_handle': None,
            'old_twitter_handle': x['handle'],
            'status': 'twitter id not found',
        })
    else:
        new_handle = api_response_data[x['twitter_id']]['screen_name']
        if x['handle'] != new_handle:
            print('{person_id}: Handle changed from @{old} to @{new}'.format(
                person_id=x['person_id'],
                old=x['handle'],
                new=new_handle,
            ))
            status = 'twitter handle updated'
            old_twitter_handle = x['handle']
        else:
            status = 'no change'
            old_twitter_handle = None
        updates.append({
            'id': x['person_id'],
            'twitter_id': x['twitter_id'],
            'twitter_handle': new_handle,
            'old_twitter_handle': old_twitter_handle,
            'status': status,
        })

# 2. If we have handles, we want to find the IDs (and check handles!)
ep_data_without_ids = {v['handle']: v for v in ep_twitter_data if not v['twitter_id']}
ids_to_find = list(ep_data_without_ids.keys())
api_response_data = {}
for lower in range(0, len(ids_to_find), 100):
    print('Fetching twitter data: {lower:,} to {upper:,} of {len:,} handles ...'.format(
        lower=lower + 1, upper=lower + 100, len=len(ids_to_find)
    ))
    screen_names = ','.join(ids_to_find[lower:lower + 100])
    payload = {'screen_name': screen_names}
    api_response_data_partial = _run_query(payload)
    if not api_response_data_partial:
        continue
    api_response_data.update({
        x['screen_name'].lower(): x for x in api_response_data_partial
    })

for x in ep_data_without_ids.values():
    if x['handle'].lower() not in api_response_data:
        # hmm - this account may have been deleted.
        # Remove the Twitter ID and handle
        print('{person_id}: Twitter handle @{handle} not found.'.format(
            person_id=x['person_id'],
            handle=x['handle'],
        ))
        updates.append({
            'id': x['person_id'],
            'twitter_id': None,
            'twitter_handle': None,
            'old_twitter_handle': x['handle'],
            'status': 'twitter handle not found',
        })
    else:
        current_twitter_user = api_response_data[x['handle'].lower()]
        new_handle = current_twitter_user['screen_name']
        new_twitter_id = current_twitter_user['id']
        if x['handle'] != new_handle:
            print('{person_id}: Handle changed from @{old} to @{new}'.format(
                person_id=x['person_id'],
                old=x['handle'],
                new=new_handle,
            ))
            status = 'twitter id added; twitter handle updated'
            old_twitter_handle = x['handle']
        else:
            status = 'twitter id added'
            old_twitter_handle = None
        print('{person_id}: Twitter ID {id_} added (@{new})'.format(
            person_id=x['person_id'],
            id_=new_twitter_id,
            new=new_handle,
        ))
        updates.append({
            'id': x['person_id'],
            'twitter_id': new_twitter_id,
            'twitter_handle': new_handle,
            'old_twitter_handle': old_twitter_handle,
            'status': status,
        })

# we always bin the old database, to get rid of all stale data.
scraperwiki.sqlite.drop()
scraperwiki.sqlite.save(['id', 'twitter_id'], updates)
