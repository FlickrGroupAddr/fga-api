import json
import boto3
import logging
import datetime
import psycopg2
import uuid


logging_level = logging.DEBUG

logger = logging.getLogger()
logger.setLevel(logging_level)

ssm = boto3.client('ssm', region_name='us-east-2' )


def _create_apigw_http_response( http_status_code, json_body, additional_headers = None ):
    # Fucking CORS is fucking me again. This isn't showing up in our responses
    return_headers = {
        'Access-Control-Allow-Origin'           : '*',
        #'Access-Control-Allow-Credentials'     : True,
    }

    if additional_headers:
        return_headers.update( additional_headers )

    return_dict = {
        "statusCode"    : http_status_code,
        "body"          : json.dumps( json_body, indent=4, sort_keys=True ), 
        "headers"       : return_headers,
    }

    return return_dict



def _read_request( event ):
    required_query_string_parameters = (
        'flickr_picture_id',
        'flickr_group_id', 
    )

    if 'queryStringParameters' in event and all( key in event['queryStringParameters'] for key in
        required_query_string_parameters ):

        response = {}

        for curr_param in required_query_string_parameters:
            response[ curr_param ] = event['queryStringParameters'][curr_param]

    else:
        response = None
 
    return response
        


def _process_api_request( api_request ):
    return  _create_apigw_http_response( 200, { "status": "valid_request_received_noop" }  )



def create_new_fga_request(event, context):
    logger.debug( json.dumps( event, indent=4, sort_keys=True) )

    try:
        api_request = _read_request( event )
        if api_request:
            logger.info( "Got new request:" )
            logger.info( json.dumps( api_request, indent=4, sort_keys=True) )

            response = _process_api_request( api_request )
        else:
            logger.warn( "No valid request found in API call, ignoring" )
            response =  _create_apigw_http_response( 400, { "error": "invalid/missing request parameters" } )

    except Exception as e:
        # If we are in debug mode, go ahead and raise the exception to give a nice
        #       stack trace for troubleshooting
        if logging_level == logging.DEBUG:
            raise e
        else:
            logger.critical("Unhandled exception caught at top level, bailing: " + str(e) )

    return response
