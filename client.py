

# ************************************************************************
# Import necessary packages
# ************************************************************************

import configparser
import json
import time
import datetime
import platform
import socket
import gzip
import random
import requests
import traceback
import base64


USE_COMPRESSION = False
VERIFY_SSL = False

WEB_REQUEST_TIMEOUT_SECONDS = 30



omfEndPoint = "https://everest.dev.osisoft.int/piwebapi/omf"
omfVersion = "1.0"

username = "enter username"
password = "enter password"



def send_omf_message_to_endpoint(message_type, message_omf_json, action='create'):
    # Sends the request out to the preconfigured endpoint..
    # Compress json omf payload, if specified
    compression = 'none'
    if USE_COMPRESSION:
        msg_body = gzip.compress(bytes(json.dumps(message_omf_json), 'utf-8'))
        compression = 'gzip'
    else:
        msg_body = json.dumps(message_omf_json)
        print(msg_body)

    msg_headers = getHeaders(compression, message_type, action)
    response = {}
    # Assemble headers
    response = requests.post(
        omfEndPoint,
        headers=msg_headers,
        data=msg_body,
        verify=VERIFY_SSL,
        timeout=WEB_REQUEST_TIMEOUT_SECONDS,
        auth=(username, password)
    )

    # Send the request, and collect the response
    print(response)
    if response.status_code == 409:
        return

    # response code in 200s if the request was successful!
    if response.status_code < 200 or response.status_code >= 300:
        print(msg_headers)
        response.close()
        print('Response from web api was bad.  "{0}" message: {1} {2}.  Message holdings: {3}'.format(message_type,
                                                                                                    response.status_code,
                                                                                                    response.text,
                                                                                                    message_omf_json))
        print()
        raise Exception(
            "OMF message was unsuccessful, {message_type}. {status}:{reason}".format(message_type=message_type,
                                                                                     status=response.status_code,
                                                                                     reason=response.text))


def getHeaders(compression="", message_type="", action=""):

    # Assemble headers   
    msg_headers = {
        'messagetype': message_type,
        'action': action,
        'messageformat': 'json',
        'omfversion': omfVersion
    }
    if (compression == "gzip"):
        msg_headers["compression"] = "gzip"

    return msg_headers


