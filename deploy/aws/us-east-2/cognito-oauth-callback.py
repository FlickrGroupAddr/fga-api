#!/usr/bin/python3

import os
import datetime
import json
import uuid
import logging
import boto3
import botocore.exceptions
import urllib
import time
import requests
import requests_oauthlib
import jwt
from requests.auth      import HTTPBasicAuth
from oauthlib.oauth2    import BackendApplicationClient
from requests_oauthlib  import OAuth2Session

logger = logging.getLogger()
logger.setLevel( logging.DEBUG )

endpoint_region = 'us-east-2'

boto3_clients = {
    'ssm'       : boto3.client( 'ssm',          region_name=endpoint_region ),
}


def cognito_oauth_callback_webui_dev(event, context):
    # Let's log our event to see what we see
    logger.debug( event )

    # If they hit our OAuth callback without a "code" URL parameter, they done fucked up, cuz that ain't the
    #       OAuth contract

    headers = None

    if 'queryStringParameters' not in event or 'code' not in event['queryStringParameters']:
        body = {
            "error": "URL to OAuth callback did not include \"code\" URL query parameter"
        }

        status_code = 400

        body = None

    else:
        auth_code = event['queryStringParameters']['code']

        ssm_params = _get_ssm_oauth_parameters( 'webui_dev' )

        logging.debug("SSM Params")
        logging.debug( json.dumps(ssm_params, indent=4, sort_keys=True) )

        #logger.debug( "Before calling exchange auth code" )
        cognito_response = _exchange_auth_code_for_bearer_token( auth_code, ssm_params )
        #logger.debug( "Back from exchange" )

        logging.debug( "Cognito response:" )
        logging.debug( json.dumps( cognito_response, indent=4, sort_keys=True) )

        session_state = {
            "cognito_session_data": {
                "tokens": {
                    "access_token"          : cognito_response['access_token'],
                    "id_token"              : cognito_response['id_token'],
                    "refresh_token"         : cognito_response['refresh_token']
                },
                "decoded_tokens"        : _decode_cognito_tokens( cognito_response ),
                "token_expiration"      : ( datetime.datetime.now(datetime.timezone.utc) + 
                    datetime.timedelta(seconds=cognito_response['expires_in']) ).isoformat( timespec='seconds' )
            }
        }

        logging.debug( "Session state" )
        logging.debug( json.dumps( session_state, indent=4, sort_keys=True) )

        # Get the user's GUID from the decoded token info
        user_cognito_id = session_state['cognito_session_data']['decoded_tokens']['id_token']['claims']['sub']

        # Cannot use HttpOnly restriction or JS can't see the cookie to send it.
        # Have to set SameSite=None until the api and ui are both under same domain
        #headers = {
            #'set-cookie'    : "GWP_USER_SESSION_ID={0}; Max-Age=3600; Domain=3ws38fheaf.execute-api.us-east-1.amazonaws.com; Path=/; Secure; SameSite=None".format( selected_guid ),
            # Expire cookie in a week
            #'set-cookie'    : f"FLICKRGROUPADDR_BEARER_TOKEN={bearer_token}; Max-Age=604800; Path=/; Secure; SameSite=None",
            #'location'      : "https://flickrgroupaddr.com/console" 
        #}

        status_code = 200

        body = { 
            "access_token": cognito_response['access_token'],
            "refresh_token": cognito_response['refresh_token'],
        }

    response = {
        "statusCode"    : status_code,
        "body"          : json.dumps( body, indent=4, sort_keys=True )
    }

    if headers is not None:
        response['headers'] = headers

    return response


def _get_ssm_oauth_parameters( app_client_id ):

    param_store_keys = (
        "/flickrgroupaddr/auth/app_client/{0}/callback_url".format( app_client_id ),
        "/flickrgroupaddr/auth/app_client/{0}/client_id".format( app_client_id ),
        "/flickrgroupaddr/auth/app_client/{0}/client_secret".format( app_client_id ),
        "/flickrgroupaddr/auth/app_client/{0}/token_url".format( app_client_id ),
    )

    return _get_ssm_params( param_store_keys )


def _get_ssm_params( param_list ):
    returned_parameters = boto3_clients['ssm'].get_parameters( Names=param_list )

    ssm_params = {}

    for curr_param in returned_parameters['Parameters']:
        # Final component after slash will be key
        param_name = curr_param['Name'].split('/')[-1]
        ssm_params[param_name] = curr_param['Value']

    return ssm_params


def _exchange_auth_code_for_bearer_token( auth_code, ssm_params ):

    logger.debug( "Auth code: {0}".format(auth_code) )

    logger.debug("Parameters retrieved from AWS Parameter Store:")
    logger.debug( json.dumps(ssm_params, indent=4, sort_keys=True) )

    if ssm_params is not None:
        # Do the OAuth2 token endpoint kabuki dance with Cognito
        auth = HTTPBasicAuth(ssm_params['client_id'], ssm_params['client_secret'] )
        oauth = OAuth2Session(
            client_id = ssm_params['client_id'],
            redirect_uri = ssm_params['callback_url'] )

        cognito_response = oauth.fetch_token(
            token_url   = ssm_params['token_url'],
            code        = auth_code,
            auth        = auth )

        return cognito_response
    else:
        return None


def _decode_cognito_tokens( cognito_response ):
    decoded_tokens = {
        'id_token': {
            'claims': jwt.decode( cognito_response['id_token'], options={ 'verify_signature': False } )
        }
    }

    return decoded_tokens


