import json
import boto3
import logging
import flickrapi
import flickrapi.auth
import datetime
import psycopg2
import uuid
import pprint


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


def _get_postgresql_creds():
    return _read_value_from_ssm( "/flickrgroupaddr/resources/pgsql/creds" )


def _get_flickr_user_creds( user_cognito_id ):
    return _read_value_from_ssm( f"/flickrgroupaddr/user/{user_cognito_id}/secrets/flickr" )


def _get_flickr_app_creds():
    return _read_value_from_ssm( "/flickrgroupaddr/resources/flickr/fga_app_api_key" )


def _read_value_from_ssm( path_string ):
    ssm_response = None

    try:
        ssm_response = json.loads(
            ssm.get_parameter(Name=path_string)['Parameter']['Value'] )

    except:
        logger.error(f"Exception thrown when trying to read SSM data at path {path_string}" )

    return ssm_response


def _create_flickr_api_handle( app_flickr_api_key_info, user_flickr_auth_info ):
    # Create an OAuth User Token that flickr API library understands
    api_access_level = "write"
    flickrapi_user_token = flickrapi.auth.FlickrAccessToken(
        user_flickr_auth_info['user_oauth_token'],
        user_flickr_auth_info['user_oauth_token_secret'],
        api_access_level,
        user_flickr_auth_info['user_fullname'],
        user_flickr_auth_info['username'],
        user_flickr_auth_info['user_nsid'])

    flickrapi_handle = flickrapi.FlickrAPI(app_flickr_api_key_info['api_key'],
                                           app_flickr_api_key_info['api_key_secret'],
                                           token=flickrapi_user_token,
                                           store_token=False,
                                           format='parsed-json')

    return flickrapi_handle


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


def _can_attempt_request( db_cursor, curr_user_request ):

    # Get most recent row of attempt status for this request -- if any
    sql_command = """
        SELECT DATE(attempt_started) AS most_recent_attempt_date, final_status AS most_recent_attempt_status
        FROM group_add_attempts
        WHERE submitted_request_fk = %s
        ORDER BY attempt_started DESC
        LIMIT 1;
    """

    sql_parameters = ( curr_user_request['user_submitted_request_id'], )
    db_cursor.execute( sql_command, sql_parameters )
    db_row = db_cursor.fetchone()
    if db_row: 
        most_recent_attempt_date = db_row[0]
        most_recent_attempt_status = db_row[1]

        #logger.debug( "Most recent date: " + pprint.pformat(most_recent_attempt_date) )
        #logger.debug( f"Most recent status: {most_recent_attempt_status}" )

        curr_date = datetime.datetime.now( datetime.timezone.utc ).date()

        # We can attempt as long as:
        #   - most recent attempt was not within the current UTC day *and*
        #   - most recent status was not a permstatus 
        can_attempt = most_recent_attempt_status.startswith("permstatus_") is False and \
            most_recent_attempt_date != curr_utc_date

    else: 
        logger.debug( f"User request {curr_user_request['user_submitted_request_id']} has never been tried, can attempt" )
        # We've never tried it, so sure!
        can_attempt = True

    logger.debug( f"Result of can attempt on request {curr_user_request['user_submitted_request_id']}: {can_attempt}" ) 

    return can_attempt



def _process_app_requests( app_requests ):
    flickr_creds_app = None

    for curr_request in app_requests:
        logger.debug( "Processing request:\n" + json.dumps(curr_request, indent=4, sort_keys=True) )

        flickr_creds_user = _get_flickr_user_creds( curr_request['user_cognito_id'] )
        if not flickr_creds_user:
            logger.warn( f"Could not find flickr creds for Cognito user ID \"{curr_request['user_cognito_id']}\", bailing" )
            continue

        # Now that we know the user is valid, do the heavier lifting
        pgsql_creds = _get_postgresql_creds()

        logger.debug( "Got PostgreSQL creds" )
        
        # By using these two "with" statements on Postgres, if we exit them without having thrown an exception, we get an auto commit
        with _generate_postgres_db_handle( pgsql_creds ) as db_handle:
            with db_handle.cursor() as db_cursor:

                # Make sure we don't have a permanent status for this request or an attempt in the current UTC day
                if _can_attempt_request( db_cursor, curr_request ) is False:
                    continue

                logger.debug( "We can attempt request, proceeding" )
        
                if flickr_creds_app is None:
                    flickr_creds_app = _get_flickr_app_creds()
                    logger.debug( "Successfully retrieved Flickr user and app creds from Parameter Store" )

                flickrapi_handle = _create_flickr_api_handle( flickr_creds_app, flickr_creds_user )
                logger.debug( "Successfully created Flickr API handle with app & user creds" )

    logger.debug( "Leaving process app requests" )




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
