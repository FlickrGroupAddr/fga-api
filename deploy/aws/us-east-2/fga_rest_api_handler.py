import json
import boto3
import botocore.exceptions
import logging
import datetime
import psycopg2
import uuid
import os
import flickrapi
import flickrapi.auth
import requests
import requests_oauthlib
import urllib
import time


logging_level = logging.DEBUG

logger = logging.getLogger()
logger.setLevel(logging_level)

ssm = boto3.client('ssm', region_name='us-east-2' )
sns = boto3.client('sns', region_name='us-east-2' )


def _create_apigw_http_response( http_status_code, json_body, additional_headers = None ):
    return_headers = {
        'Content-Type'                          : 'application/json',
        'Access-Control-Allow-Origin'           : '*',
        #'Access-Control-Allow-Credentials'     : True,
    }

    if additional_headers:
        return_headers.update( additional_headers )

    return_dict = {
        "statusCode"    : http_status_code,
        "body"          : json.dumps( json_body, indent=4, sort_keys=True ), 
        #"headers"       : return_headers,
    }

    return return_dict



def _read_request( event ):
    required_query_string_parameters = (
        'flickr_picture_id',
        'flickr_group_id', 
    )

    if 'queryStringParameters' in event \
            and all( key in event['queryStringParameters'] for key in required_query_string_parameters ) \
            and 'headers' in event \
            and 'authorization' in event['headers']:

        response = {}

        for curr_param in required_query_string_parameters:
            response[ curr_param ] = event['queryStringParameters'][curr_param]

        response[ 'user_cognito_id' ] = event['headers']['authorization']
    else:
        response = None
 
    return response


def _read_value_from_ssm( path_string ):
    ssm_response = None

    try:
        ssm_response = json.loads(
            ssm.get_parameter(Name=path_string)['Parameter']['Value'] )

    except:
        logger.warn(f"Exception thrown when trying to read SSM data at path {path_string}" )

    return ssm_response
        

def _get_postgresql_creds():
    return _read_value_from_ssm( "/flickrgroupaddr/resources/pgsql/creds" )


def _get_flickr_app_creds():
    return _read_value_from_ssm( "/flickrgroupaddr/resources/flickr/fga_app_api_key" )


def _get_flickr_user_perms_granted_callback():
    return _read_value_from_ssm( "/flickrgroupaddr/resources/flickr/user-permission-granted-callback" ) 

def _generate_postgres_db_handle( pgsql_creds ):
    #logger.debug( "Attempting DB connect" )
    #logger.debug( json.dumps(pgsql_creds, indent=4, sort_keys=True) )

    db_handle = psycopg2.connect(
        host        = pgsql_creds['db_host'],
        user        = pgsql_creds['db_user'],
        password    = pgsql_creds['db_passwd'],
        database    = pgsql_creds['database_name'] )

    #logger.debug( "Back from DB connect" )

    return db_handle


def _get_flickr_user_creds( user_cognito_id ):
    return _read_value_from_ssm( f"/flickrgroupaddr/user/{user_cognito_id}/secrets/flickr" )


def _do_sns_notify( api_request, success_body ):
    logger.debug( "Starting SNS notification due to successful DB insert" )

    # let's see if the attempt to store the ARN of the generated SNS topic by serverless in env var worked
    sns_topic_arn = os.getenv( 'SNS_TOPIC_ARN' )
    if sns_topic_arn:
        logger.debug( f"SNS Topic ARN was found in env vars: {sns_topic_arn}, sending notification" )
        try:
            request_guid        = json.loads( success_body )['fga_request_guid']
            user_cognito_id     = api_request['user_cognito_id']
            flickr_picture_id   = api_request['flickr_picture_id']
            flickr_group_id     = api_request['flickr_group_id']

            sns_notification = {
  	            "user_submitted_request_id"     : request_guid,
	            "user_cognito_id"               : user_cognito_id,
	            "flickr_picture_id"             : flickr_picture_id,
	            "flickr_group_id"               : flickr_group_id,
            }

            logger.debug( "SNS notification text" )
            sns_notification_text = json.dumps( sns_notification, indent=4, sort_keys=True )
            logger.debug( sns_notification_text )

            sns.publish( 
                TopicArn    = sns_topic_arn,
                Message     = sns_notification_text,
            )

            logging.info( "Successfully published notification of new FGA request to SNS" )
        except Exception as e:
            if logging_level == logging.DEBUG:
                raise e
            else:
                logger.warn( f"Exception thrown when trying to publish to SNS topic: {str(e)}" )
    else:
        logger.warn( "Could not find SNS topic ARN in env var, cannot post to SNS" )


def _process_api_request( api_request ):

    response = _do_db_insert( api_request )

    if response["statusCode"] == 200:
        # Posting to SNS is a nice to have, failure at this point is invisible to the caller
        _do_sns_notify( api_request, response['body'] )

    return response


def _do_db_insert( api_request ):
    flickr_creds_user = _get_flickr_user_creds( api_request['user_cognito_id'] )
    if not flickr_creds_user:
        logger.warn( f"Could not find flickr creds for Cognito user ID \"{api_request['user_cognito_id']}\", bailing" )
        response = _create_apigw_http_response( 401, { "error": "Invalid Authorization header" }  )
    else:
        pgsql_creds = _get_postgresql_creds()
        logger.debug( "Got PostgreSQL creds" ) 

        # By using these two "with" statements on Postgres, if we exit them without having thrown an exception, 
        #       we get an auto commit
        with _generate_postgres_db_handle( pgsql_creds ) as db_handle:
            with db_handle.cursor() as db_cursor:
                add_attempt_guid    = str( uuid.uuid4() )
                user_cognito_id     = api_request['user_cognito_id']
                flickr_picture_id   = api_request['flickr_picture_id']
                flickr_group_id     = api_request['flickr_group_id']
                current_timestamp   = datetime.datetime.now( datetime.timezone.utc ) 

                sql_command = """
                    INSERT INTO submitted_requests (uuid_pk, flickr_user_cognito_id, picture_flickr_id, flickr_group_id, 
                        request_datetime) 
                    VALUES ( %s, %s, %s, %s, %s )
                    RETURNING uuid_pk;
                """

                sql_params = (
                    add_attempt_guid,
                    user_cognito_id,
                    flickr_picture_id,
                    flickr_group_id,
                    current_timestamp )

                
                try:
                    db_cursor.execute( sql_command, sql_params )

                    add_attempt_guid_row = db_cursor.fetchone()
                    if add_attempt_guid_row:
                        add_attempt_guid = add_attempt_guid_row[0]
                        response = _create_apigw_http_response( 
                            200, 
                            { 
                                "fga_request_guid": str(add_attempt_guid),
                            }  
                        )

                        logging.info( f"Successful request, added to DB, assigned GUID {str(add_attempt_guid)}" )

                    else:
                        response =  _create_apigw_http_response( 500, 
                            { 
                                "error": "no GUID returned from DB insert",
                            }
                        )
                except psycopg2.Error as e:
                    logging.warn( f"Operation failed, DB exception thrown: {str(e)}" )
                    response = _create_apigw_http_response( 
                        500,
                        {
                            "error"             : "DB exception thrown",
                            "error_details"     : str( e ),
                        }
                    )

                    
    return response


def _get_cognito_user_id_from_event( event ):
    if 'requestContext' in event and 'authorizer' in event['requestContext'] \
            and 'jwt' in event['requestContext']['authorizer'] \
            and 'claims' in event['requestContext']['authorizer']['jwt'] \
            and 'sub' in event['requestContext']['authorizer']['jwt']['claims']:

        cognito_user_id = event['requestContext']['authorizer']['jwt']['claims']['sub']
    else:
        logger.error("Could not find cognito user ID in event object" )
        cognito_user_id = None

    return cognito_user_id



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
            response =  _create_apigw_http_response( 400, { "error": "invalid/missing request parameters or request headers" } )

    except Exception as e:
        # If we are in debug mode, go ahead and raise the exception to give a nice
        #       stack trace for troubleshooting
        if logging_level == logging.DEBUG:
            raise e
        else:
            logger.critical("Unhandled exception caught at top level, bailing: " + str(e) )

    return response


def get_flickr_id( event, context):
    logger.debug( json.dumps( event, indent=4, sort_keys=True) )

    try:
        cognito_user_id = _get_cognito_user_id_from_event( event ) 
        logger.info( f"Authenticated Cognito user: {cognito_user_id}" )

        flickr_creds = _get_flickr_user_creds( cognito_user_id ) 

        if flickr_creds is not None:

            response_body = { 
                "username"      : flickr_creds['username'],
                "user_nsid"     : flickr_creds['user_nsid'],
            } 
            response = _create_apigw_http_response( 200, response_body )

        else:
            response = _create_apigw_http_response( 404, None )


    except Exception as e:
        # If we are in debug mode, go ahead and raise the exception to give a nice
        #       stack trace for troubleshooting
        if logging_level == logging.DEBUG:
            raise e
        else:
            logger.critical("Unhandled exception caught at top level, bailing: " + str(e) )
            response = _create_apigw_http_response( 500, None )

    return response


def _get_dynamodb_table_handle(): 
    table_name = "flickrgroupaddr"
    endpoint_region = "us-east-2"

    # Make the connection
    try:
        table_handle = boto3.resource(
            'dynamodb', region_name=endpoint_region).Table( table_name )
    except e:
        logger.critical("Could not establish handle to DynamoDB table {0}".format(table_name) )

    return table_handle


def _store_flickr_perms_callback_state_in_dynamo( cognito_user_id, oauth_token, oauth_secret ):
    logger.debug( f"Requested to store state for Flickr perms granted callback in dynamo DB" )

    dynamodb_handle = _get_dynamodb_table_handle()

    try:
        # Compute unix timestamp ten mins from now
        ten_mins_in_seconds = 600
        ttl_value = int( time.time() ) + ten_mins_in_seconds

        dynamodb_handle.put_item(
            Item={
                'PK'                : f"oauth_token_{oauth_token}",
                'SK'                : "flickr_permissions_granted_callback_state",
                'cognito_user_id'   : cognito_user_id,
                'oauth_secret'      : oauth_secret,
                'ttl'               : ttl_value,
            }
        )

        logger.info( f"State for Flickr perms granted callback stored for oauth_token {oauth_token}: oauth_secret = {oauth_secret}, user cognito id = {cognito_user_id}, ttl = {ttl_value} (10 mins from now)" )

    except botocore.exceptions.ClientError as e:
        error_msg = e.response['Error']['Message']

        logger.error( f"Exception thrown when trying to add perms granted callback state, response: {error_msg}" )
        if logging_level == logging.DEBUG:
            raise e


def put_flickr_id( event, context ):
    logger.debug( json.dumps( event, indent=4, sort_keys=True) )

    try:
        cognito_user_id = _get_cognito_user_id_from_event( event )
        logger.info( f"Authenticated Cognito user: {cognito_user_id}" )

        flickr_app_creds = _get_flickr_app_creds()

        logger.debug("Flickr App Creds:")
        logger.debug( json.dumps(flickr_app_creds, indent=4, sort_keys=True) )

        flickr = flickrapi.FlickrAPI(
            flickr_app_creds['api_key'],
            flickr_app_creds['api_key_secret'],
            store_token = False)

        permissions_granted_callback_url = _get_flickr_user_perms_granted_callback()

        logger.debug( f"Permissions granted callback: {permissions_granted_callback_url}" )

        if permissions_granted_callback_url is None:
            logger.warn("Having to manual overwrite callback but I'm bored" )
            permissions_granted_callback_url = "https://x4etaszxrl.execute-api.us-east-2.amazonaws.com/api/v001/flickr/user-permission-granted-callback"

        flickr.get_request_token(oauth_callback=permissions_granted_callback_url)

        auth_url = flickr.auth_url( perms='write' )

        # Extract the oauth_token from the generated URL we're about to bounce the user
        # to
        parsed_url = urllib.parse.urlparse( auth_url )
        oauth_token = urllib.parse.parse_qs( parsed_url.query )['oauth_token'][0]
        logger.debug( f"OAuth token: {oauth_token}" )

        # Rudely reach in without accessors (as the flickrapi API doesn't expose a function to
        #       get it) and pull out the oauth_secret from the data returned by the request 
        #       token call. 
        #
        #       This data needs to be stored in dynamo for the resulting perms callback to get
        #       the state needed to get a Flickr access_token
        oauth_secret = flickr.flickr_oauth.oauth.client.resource_owner_secret
        logger.debug( f"OAuth secret: {oauth_secret}" )

        _store_flickr_perms_callback_state_in_dynamo( cognito_user_id, oauth_token, oauth_secret )

        response_body = { "flickr_auth_url": auth_url }
        response = _create_apigw_http_response( 200, response_body )


    except Exception as e:
        # If we are in debug mode, go ahead and raise the exception to give a nice
        #       stack trace for troubleshooting
        if logging_level == logging.DEBUG:
            raise e
        else:
            logger.critical("Unhandled exception caught at top level, bailing: " + str(e) )
            response = _create_apigw_http_response( 500, None )

    return response



def _get_perms_granted_callback_state( oauth_token ):
    logger.debug( f"Requested to retrieve state for Flickr perms granted callback for oauth_token {oauth_token}" )

    dynamodb_handle = _get_dynamodb_table_handle()

    try:
        dynamo_response = dynamodb_handle.get_item(
            Key={
                'PK'    : f"oauth_token_{oauth_token}",
                'SK'    : "flickr_permissions_granted_callback_state",
            },

            ConsistentRead=True
        )

        logger.info( f"Successful DynamoDB pull for oauth_token {oauth_token}" )

        if dynamo_response is not None and 'Item' in dynamo_response:
            return_state = {
                'oauth_secret'      : dynamo_response['Item']['oauth_secret'],
                'cognito_user_id'   : dynamo_response['Item']['cognito_user_id'],
            }
        else:
            return_state = None

    except botocore.exceptions.ClientError as e:
        error_msg = e.response['Error']['Message']

        logger.error( f"Exception thrown when trying to get perms granted callback state for oauth token {oauth_token}, error message: {error_msg}" )
        if logging_level == logging.DEBUG:
            raise e
        else:
            return_state = None

    return return_state 


def _get_flickr_access_token_endpoint_url():
    # TODO: change this to SSM
    return "https://www.flickr.com/services/oauth/access_token"


def _store_user_access_token( cognito_user_id, flickr_access_token ):
    param_store_key = f"/flickrgroupaddr/user/{cognito_user_id}/secrets/flickr"

    ssm.put_parameter(
        Name        = param_store_key,
        Value       = json.dumps( flickr_access_token, indent=4, sort_keys=True ),
        Type        = "String",

        # Find to allow overwrite, they may have re-authed
        Overwrite   = True
    )


def user_permission_granted_callback( event, context ):
    logger.debug( json.dumps( event, indent=4, sort_keys=True) )

    try:
        #response =  _create_apigw_http_response( 200, event )
        # Pull the two parameters that we should have
        if 'queryStringParameters' in event \
                and 'oauth_token' in event['queryStringParameters'] \
                and 'oauth_verifier' in event['queryStringParameters']:

            # Pull our callback state from dynamo

            flickr_oauth_data = {
                'oauth_token'       : event['queryStringParameters']['oauth_token'],
                'oauth_verifier'    :  event['queryStringParameters']['oauth_verifier']
            }

            # Pull our callback state from dynamo, keyed by oauth_token
            perms_granted_callback_state = _get_perms_granted_callback_state(
                flickr_oauth_data['oauth_token'] )

            if perms_granted_callback_state is None:
                response = _create_apigw_http_response( 400, 
                    {
                        "error": f"Invalid perms callback invocation, no callback state found for oauth_token {flickr_oauth_data['oauth_token']}" 
                    }
                )
            else:

                flickr_app_creds = _get_flickr_app_creds()

                logger.debug("Flickr App Creds:")
                logger.debug( json.dumps(flickr_app_creds, indent=4, sort_keys=True) )

                # Create the oauth1 object that will be used to get us our access token
                api_key         = flickr_app_creds['api_key']
                api_secret      = flickr_app_creds['api_key_secret']
                oauth_token     = flickr_oauth_data['oauth_token']
                oauth_secret    = perms_granted_callback_state['oauth_secret']
                verifier        = flickr_oauth_data['oauth_verifier']
                
                oauth_obj =  requests_oauthlib.OAuth1(
                    client_key              = api_key,
                    client_secret           = api_secret,
                    resource_owner_key      = oauth_token,
                    resource_owner_secret   = oauth_secret,
                    verifier                = verifier )

                access_token_endpoint_url = _get_flickr_access_token_endpoint_url()

                access_token_response = requests.post( access_token_endpoint_url, auth = oauth_obj )

                if access_token_response.status_code != 200:
                    logger.error( f"Non-200 status code {access_token_response.status_code} returned" )

                    response = _create_apigw_http_response( 500, 
                        {
                            "error": "request for access token was rejected by flickr"
                        }
                    )

                else:
                    logger.info( "Got a valid access token back from Flickr!" ) 

                    cognito_user_id = perms_granted_callback_state['cognito_user_id']

                    logger.debug( f"Access token content for Cognito user {cognito_user_id}:\n{access_token_response.content}" )

                    character_encoding = 'utf-8'
                    access_token_string_uri_encoded = access_token_response.content.decode( 
                        character_encoding )
                    #parsed_access_token = urllib.parse.urlparse( auth_url )
                    access_token = urllib.parse.parse_qs( access_token_string_uri_encoded )

                    for curr_key in access_token:
                        access_token[curr_key] = access_token[curr_key][0]

                    _store_user_access_token( cognito_user_id, access_token )

                    response = _create_apigw_http_response( 200, 
                        { 
                            "access_token"  : access_token,
                        }
                    )

        else:
            response = _create_apigw_http_response( 400, 
                {
                    "error"     : "callback did not include oauth token/verifier" 
                }
            )



    except Exception as e:
        # If we are in debug mode, go ahead and raise the exception to give a nice
        #       stack trace for troubleshooting
        if logging_level == logging.DEBUG:
            raise e
        else:
            logger.critical("Unhandled exception caught at top level, bailing: " + str(e) )
            response = _create_apigw_http_response( 500, None )

    return response
