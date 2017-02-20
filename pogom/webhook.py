#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import requests
from datetime import datetime
from requests_futures.sessions import FuturesSession
import threading
from .utils import get_args
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import json

log = logging.getLogger(__name__)

# How low do we want the queue size to stay?
wh_warning_threshold = 100
# How long can it be over the threshold, in seconds?
# Default: 5 seconds per 100 in threshold.
wh_threshold_lifetime = int(5 * (wh_warning_threshold / 100.0))
wh_lock = threading.Lock()

globalDefault = 100
globalFilter = {
1: 90,
2: 0,
3: 0,
4: 90,
5: 0,
6: 0,
7: 90,
8: 0,
9: 0,
25: 90,
26: 0,
31: 0,
34: 0,
38: 0,
43: 90,
44: 0,
45: 0,
58: 90,
59: 0,
60: 90,
61: 0,
62: 0,
63: 90,
64: 0,
65: 0,
66: 90,
67: 0,
68: 0,
69: 90,
70: 0,
71: 0,
74: 90,
75: 0,
76: 0,
79: 90,
80: 0,
88: 0,
89: 0,
94: 0,
95: 90,
102: 90,
103: 0,
106: 0,
107: 0,
108: 90,
111: 90,
112: 0,
113: 0,
114: 0,
123: 90,
124: 90,
125: 90,
126: 90,
127: 90,
129: 98,
130: 0,
131: 0,
132: 0,
133: 90,
134: 0,
135: 0,
136: 0,
137: 0,
138: 90,
139: 0,
140: 90,
141: 0,
142: 0,
143: 0,
144: 0,
145: 0,
146: 0,
147: 0,
148: 0,
149: 0,
150: 0,
151: 0,
152: 80,
153: 0,
154: 0,
155: 80,
156: 0,
157: 0,
158: 80,
159: 0,
160: 0,
169: 80,
170: 80,
171: 80,
172: 80,
173: 80,
174: 80,
175: 80,
176: 0,
178: 80,
179: 0,
180: 0,
181: 0,
182: 0,
183: 90,
184: 0,
185: 80,
186: 0,
187: 90,
188: 0,
189: 0,
190: 90,
191: 50,
192: 0,
193: 50,
194: 90,
195: 50,
196: 0,
197: 0,
199: 0,
200: 90,
201: 0,
202: 80,
203: 80,
204: 80,
205: 50,
206: 50,
207: 80,
208: 0,
210: 50,
211: 90,
212: 0,
213: 90,
214: 0,
215: 90,
216: 80,
217: 0,
218: 50,
219: 0,
221: 90,
222: 50,
223: 80,
224: 0,
225: 0,
226: 80,
227: 80,
228: 90,
229: 0,
230: 0,
231: 80,
232: 0,
233: 0,
234: 50,
235: 0,
236: 0,
237: 0,
238: 90,
239: 90,
240: 90,
241: 0,
242: 0,
243: 0,
244: 0,
245: 0,
246: 0,
247: 0,
248: 0,
249: 0,
250: 0,
251: 0
}

def send_to_webhook(session, message_type, message):
    args = get_args()

    if not args.webhooks:
        # What are you even doing here...
        log.warning('Called send_to_webhook() without webhooks.')
        return

    req_timeout = args.wh_timeout

    if not message_type == 'pokemon':
        log.info('Got post for non pokemon ' + message_type)
        return

    id = message.get("pokemon_id")
    if not id:
        log.info('Got post with no id ' + str(message))
        return

    shiny = message.get("shiny", False)
    if id in args.encounter_blacklist and not shiny:
        log.debug('filtering due to no IV or not shiny')
        return

    iv = ((int(message["individual_attack"]) + int(message["individual_defense"]) + int(message["individual_stamina"])) * 100) / 45

    requirediv = globalFilter.get(id, globalDefault)
    if iv < requirediv and not shiny:
        log.info("PokemonID: " + str(id) + " | IV: " + str(iv) + " (too Low) | Required: " + str(requirediv) + ".")
        return

    jsonMessage = json.dumps(message)
    data = {
        'content': jsonMessage
    }

    for w in args.webhooks:
        try:
            session.post(w, json=data, timeout=(None, req_timeout),
                         background_callback=__wh_completed)
        except requests.exceptions.ReadTimeout:
            log.exception('Response timeout on webhook endpoint %s.', w)
        except requests.exceptions.RequestException as e:
            log.exception(repr(e))


def wh_updater(args, queue, key_cache):
    wh_threshold_timer = datetime.now()
    wh_over_threshold = False

    # Set up one session to use for all requests.
    # Requests to the same host will reuse the underlying TCP
    # connection, giving a performance increase.
    session = __get_requests_session(args)

    # The forever loop.
    while True:
        try:
            # Loop the queue.
            whtype, message = queue.get()

            # Extract the proper identifier.
            ident_fields = {
                'pokestop': 'pokestop_id',
                'pokemon': 'encounter_id',
                'gym': 'gym_id'
            }
            ident = message.get(ident_fields.get(whtype), None)

            # cachetools in Python2.7 isn't thread safe, so we add a lock.
            with wh_lock:
                # Only send if identifier isn't already in cache.
                if ident is None:
                    # We don't know what it is, so let's just log and send
                    # as-is.
                    log.debug(
                        'Sending webhook item of unknown type: %s.', whtype)
                    send_to_webhook(session, whtype, message)
                elif ident not in key_cache:
                    key_cache[ident] = message
                    log.debug('Sending %s to webhook: %s.', whtype, ident)
                    send_to_webhook(session, whtype, message)
                else:
                    # Make sure to call key_cache[ident] in all branches so it
                    # updates the LFU usage count.

                    # If the object has changed in an important way, send new
                    # data to webhooks.
                    if __wh_object_changed(whtype, key_cache[ident], message):
                        key_cache[ident] = message
                        send_to_webhook(session, whtype, message)
                        log.debug('Sending updated %s to webhook: %s.',
                                  whtype, ident)
                    else:
                        log.debug('Not resending %s to webhook: %s.',
                                  whtype, ident)

            # Webhook queue moving too slow.
            if (not wh_over_threshold) and (
                    queue.qsize() > wh_warning_threshold):
                wh_over_threshold = True
                wh_threshold_timer = datetime.now()
            elif wh_over_threshold:
                if queue.qsize() < wh_warning_threshold:
                    wh_over_threshold = False
                else:
                    timediff = datetime.now() - wh_threshold_timer

                    if timediff.total_seconds() > wh_threshold_lifetime:
                        log.warning('Webhook queue has been > %d (@%d);'
                                    + ' for over %d seconds,'
                                    + ' try increasing --wh-concurrency'
                                    + ' or --wh-threads.',
                                    wh_warning_threshold,
                                    queue.qsize(),
                                    wh_threshold_lifetime)

            queue.task_done()
        except Exception as e:
            log.exception('Exception in wh_updater: %s.', repr(e))


# Helpers

# Background handler for completed webhook requests.
# Currently doesn't do anything.
def __wh_completed():
    pass


def __get_requests_session(args):
    # Config / arg parser
    num_retries = args.wh_retries
    backoff_factor = args.wh_backoff_factor
    pool_size = args.wh_concurrency

    # Use requests & urllib3 to auto-retry.
    # If the backoff_factor is 0.1, then sleep() will sleep for [0.1s, 0.2s,
    # 0.4s, ...] between retries. It will also force a retry if the status
    # code returned is 500, 502, 503 or 504.
    session = FuturesSession(max_workers=pool_size)

    # If any regular response is generated, no retry is done. Without using
    # the status_forcelist, even a response with status 500 will not be
    # retried.
    retries = Retry(total=num_retries, backoff_factor=backoff_factor,
                    status_forcelist=[500, 502, 503, 504])

    # Mount handler on both HTTP & HTTPS.
    session.mount('http://', HTTPAdapter(max_retries=retries,
                                         pool_connections=pool_size,
                                         pool_maxsize=pool_size))
    session.mount('https://', HTTPAdapter(max_retries=retries,
                                          pool_connections=pool_size,
                                          pool_maxsize=pool_size))

    return session


def __get_key_fields(whtype):
    key_fields = {
        # lure_expiration is a UTC timestamp so it's good (Y).
        'pokestop': ['enabled', 'latitude',
                     'longitude', 'lure_expiration', 'active_fort_modifier'],
        'pokemon': ['spawnpoint_id', 'pokemon_id', 'latitude', 'longitude',
                    'disappear_time', 'move_1', 'move_2',
                    'individual_stamina', 'individual_defense',
                    'individual_attack'],
        'gym': ['team_id', 'guard_pokemon_id',
                'gym_points', 'enabled', 'latitude', 'longitude']
    }

    return key_fields.get(whtype, [])


# Determine if a webhook object has changed in any important way (and
# requires a resend).
def __wh_object_changed(whtype, old, new):
    # Only test for important fields: don't trust last_modified fields.
    fields = __get_key_fields(whtype)

    if not fields:
        log.debug('Received an object of unknown type %s.', whtype)
        return True

    return not __dict_fields_equal(fields, old, new)


# Determine if two dicts have equal values for all keys in a list.
def __dict_fields_equal(keys, a, b):
    for k in keys:
        if a.get(k) != b.get(k):
            return False

    return True
