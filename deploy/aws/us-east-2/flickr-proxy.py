import json
import boto3
import logging
import flickrapi
import flickrapi.auth
import datetime
import psycopg2
import uuid


logging_level = logging.DEBUG

logger = logging.getLogger()
logger.setLevel(logging_level)

ssm = boto3.client('ssm', region_name='us-east-2' )


def _valid_app_request( app_request ):
    required_keys = (
        "user_submitted_request_id",
        "user_cognito_id",
        "flickr_picture_id",
        "flickr_group_id"
    )

    return all( key in app_request for key in required_keys )


def _read_event(event):
    app_requests = []
    if 'Records' in event:
        for curr_record in event['Records']:
            if 'Sns' in curr_record:
                if 'Message' in curr_record['Sns']:
                    try:
                        app_request = json.loads( curr_record['Sns']['Message'] )
                        if _valid_app_request( app_request ):
                            app_requests.append( app_request )
                        else:
                            logger.warn("Invalid app request, ignoring:" )
                            logger.warn(json.dumps(app_request, indent=4, sort_keys=True) )
                    except:
                        logger.warn("Exception thrown when parsing JSON out of event message, ignoring message:\n" +
                            curr_record['Sns']['Message'] )

    return app_requests


def _get_flickr_user_creds( user_cognito_id ):
    user_creds = None

    try:
        user_creds = json.loads(
            ssm.get_parameter(Name=f"/flickrgroupaddr/user/{user_cognito_id}/secrets/flickr_oauth_tokens" )['Parameter']['Value'] )

        logger.info( f"Cognito user ID {user_cognito_id} successfully returned flickr creds from Param Store" )
        logger.debug( json.dumps(user_creds, indent=4, sort_keys=True) )


    except:
        logger.error(f"Exception thrown when trying to pull user creds for cognito user {user_cognito_id}" )

    return user_creds


def _process_app_requests( app_requests ):
    for curr_request in app_requests:
        logger.debug( "Processing request:\n" + json.dumps(curr_request, indent=4, sort_keys=True) )

        
        flickr_creds = _get_flickr_user_creds( curr_request['user_cognito_id'] )
        if not flickr_creds:
            logger.warn( f"Could not find flickr creds for Cognito user ID \"{curr_request['user_cognito_id']}\", bailing" )
            continue


def attempt_flickr_group_add(event, context):
    logger.debug( json.dumps( event, indent=4, sort_keys=True) )
    #print( json.dumps( event, indent=4, sort_keys=True ) )

    try:
        app_requests = _read_event( event )
        logger.debug( "Got app_requests:" )
        logger.debug( json.dumps( app_requests, indent=4, sort_keys=True) )

        _process_app_requests( app_requests )
    except Exception as e:
        logger.error("Unhandled exception caught at top level, bailing: " + str(e) )
