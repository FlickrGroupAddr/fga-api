import json
import boto3
import logging
import datetime
import psycopg2
import uuid
import os


logging_level = logging.DEBUG

logger = logging.getLogger()
logger.setLevel(logging_level)

ssm = boto3.client('ssm', region_name='us-east-2' )
sns = boto3.client('sns', region_name='us-east-2' )


def _create_apigw_http_response( http_status_code, json_body, additional_headers = None ):
    # Fucking CORS is fucking me again. This isn't showing up in our responses
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
        logger.error(f"Exception thrown when trying to read SSM data at path {path_string}" )

    return ssm_response
        

def _get_postgresql_creds():
    return _read_value_from_ssm( "/flickrgroupaddr/resources/pgsql/creds" )


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
