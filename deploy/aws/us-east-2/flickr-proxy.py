import json
import boto3
import logging
import flickrapi
import flickrapi.auth
import datetime
import uuid
import pprint
import os


logging_level = logging.DEBUG

logger = logging.getLogger()
logger.setLevel(logging_level)

ssm = boto3.client('ssm', region_name='us-east-2' )
sqs = boto3.client('sqs', region_name='us-east-2' )

SQS_QUEUE_URL = os.getenv( 'SQS_QUEUE_URL' )


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

        curr_utc_date = datetime.datetime.now( datetime.timezone.utc ).date()

        # We can attempt as long as:
        #   - most recent attempt was not within the current UTC day *and*
        #   - most recent status was not a permstatus 
        has_permstatus = most_recent_attempt_status.startswith("permstatus_")
        too_soon = most_recent_attempt_date == curr_utc_date
        can_attempt = has_permstatus is False and too_soon is False
        if can_attempt is False:
            logger.info( f"Cannot attempt request: has_permstatus={has_permstatus}, too_soon={too_soon}, ignoring request" )

    else: 
        logger.debug( f"User request {curr_user_request['user_submitted_request_id']} has never been tried, can attempt" )
        # We've never tried it, so sure!
        can_attempt = True

    logger.debug( f"Result of can attempt on request {curr_user_request['user_submitted_request_id']}: {can_attempt}" ) 

    return can_attempt


def _get_group_memberships_for_user( flickrapi_handle ):
    return_groups = None

    try:
        user_groups = flickrapi_handle.groups.pools.getGroups()

        #logger.debug( "User memberships:\n" + json.dumps(user_groups, indent=4, sort_keys=True))
        if 'groups' in user_groups and 'group' in user_groups['groups']:
            return_groups = {}
            for curr_group in user_groups['groups']['group']:
                #logger.debug("Processing group:\n" + json.dumps(curr_group, indent=4, sort_keys=True) )
                if 'id' in curr_group:
                    return_groups[curr_group['id']] = None
    except Exception as e:
        if logging_level == logging.DEBUG:
            raise e
        else:
            logging.warn( f"Exception thrown when getting group memberships for user: {str(e)}" )

    return return_groups


def _get_group_memberships_for_pic( flickrapi_handle, pic_id ):
    group_memberships = None
    try:
        pic_contexts = flickrapi_handle.photos.getAllContexts( photo_id=pic_id )
        logger.debug( "Contexts:\n" + json.dumps(pic_contexts, indent=4, sort_keys=True))
        group_memberships = {}
        if 'pool' in pic_contexts:
            for curr_group in pic_contexts['pool']:
                group_memberships[ curr_group['id']] = curr_group

        logger.debug( "Group memberships:\n" + json.dumps(group_memberships, indent=4, sort_keys=True))

    except Exception as e:
        if logging_level == logging.DEBUG:
            raise e
        else:
            logging.warn( f"Exception thrown when trying to get Flickr groups for pic {pic_id}: {str(e)}" )

    return group_memberships


def _perform_group_add( flickrapi_handle, photo_id, group_id ):
    current_timestamp = datetime.datetime.now( datetime.timezone.utc ).replace( microsecond=0 )

    try:
        logger.info(f"Attempting to add photo {photo_id} to group {group_id}")
        flickrapi_handle.groups.pools.add( photo_id=photo_id, group_id=group_id )

        # Success!
        logger.info( f"Successful attempt to add photo {photo_id} to group {group_id}!" )
        operation_status = 'permstatus_success_added' 

    except flickrapi.exceptions.FlickrError as e:
        error_string = str(e)
        group_throttled_msg = "Error: 5:"
        adding_to_pending_queue_error_msg = "Error: 6:"
        if error_string.startswith(group_throttled_msg):
            operation_status = 'defer_group_throttled_for_user'
            logger.info(f"This user is throttled by group {group_id}" )
        elif error_string.startswith(adding_to_pending_queue_error_msg):
            operation_status = 'permstatus_success_added_queued'
            logger.info( f"Success (message added to queue for admin review)" )
        else:
            logger.warn( f"Unexpected error: {error_string}" )
            operation_status = f"fail_{error_string}" 

    return operation_status


def _attempt_flickr_add( curr_user_request, flickrapi_handle ):

    timestamp_start = datetime.datetime.now( datetime.timezone.utc )

    group_memberships_for_user = _get_group_memberships_for_user( flickrapi_handle )
    group_memberships_for_pic = _get_group_memberships_for_pic( flickrapi_handle, curr_user_request['flickr_picture_id'] )

    if group_memberships_for_user is None or group_memberships_for_pic is None:
        logger.warn("Exception thrown when pulling groups for group or user, bailing on attempt" )
        return

    #logger.debug( "User memberships:" )
    #logger.debug( json.dumps(group_memberships_for_user, indent=4, sort_keys=True) )
    #logger.debug( "Pic group memberships:" )
    #logger.debug( json.dumps(group_memberships_for_pic, indent=4, sort_keys=True) )

    if SQS_QUEUE_URL is None:
        logger.error("No SQS queue to publish results to, bailing" )
        return

    # If the user isn't in the requested group, mark a permfail
    if curr_user_request['flickr_group_id'] not in group_memberships_for_user:

        logger.debug( "User requested a picture be added into a group they are not in" )
        attempt_status = "permstatus_fail_user_not_in_flickr_group"

    # If this pic is already in the requested group, skip it
    elif curr_user_request['flickr_group_id'] in group_memberships_for_pic:

        logger.debug( f"Pic {curr_user_request['flickr_picture_id']} already in group " +
            f"{curr_user_request['flickr_group_id']}" )

        attempt_status = "permstatus_success_pic_already_in_group"

    else:
        results_of_add_attempt = _perform_group_add( 
            flickrapi_handle, 
            curr_user_request['flickr_picture_id'],
            curr_user_request['flickr_group_id'] )

        attempt_status = results_of_add_attempt

    logger.info( f"Final Flickr operation status: {attempt_status}" )

    timestamp_end           = datetime.datetime.now( datetime.timezone.utc )

    finished_attempt_msg    = {
        "user_submitted_request_id"     : curr_user_request['user_submitted_request_id'],
        "timestamp_attempt_started"     : timestamp_start.isoformat(),
        "timestamp_attempt_ended"       : timestamp_end.isoformat(),
        "final_status"                  : attempt_status,
    }


    sqs.send_message(
        QueueUrl        = SQS_QUEUE_URL,
        MessageBody     = json.dumps(finished_attempt_msg, indent=4, sort_keys=True),
        MessageGroupId  = curr_user_request['user_submitted_request_id'] )
 

def _process_app_requests( app_requests ):
    flickr_creds_app = _get_flickr_app_creds()
    logger.debug( "Successfully retrieved Flickr user and app creds from Parameter Store" )



    for curr_request in app_requests:
        logger.info( "Processing request:\n" + json.dumps(curr_request, indent=4, sort_keys=True) )

        flickr_creds_user = _get_flickr_user_creds( curr_request['user_cognito_id'] )
        if not flickr_creds_user:
            logger.warn( f"Could not find flickr creds for Cognito user ID \"{curr_request['user_cognito_id']}\", bailing" )
            continue

        flickrapi_handle = _create_flickr_api_handle( flickr_creds_app, flickr_creds_user )
        logger.debug( "Successfully created Flickr API handle with app & user creds" )

        # Make sure we don't have a permanent status for this request or an attempt in the current UTC day

        # Can't actually access Postgres directly anymore, so the periodic poller will need to do this logic
        #if _can_attempt_request( db_cursor, curr_request ) is False:
        #    continue

        _attempt_flickr_add( curr_request, flickrapi_handle )

    logger.debug( "Leaving process app requests" )




def attempt_flickr_group_add(event, context):
    logger.debug( json.dumps( event, indent=4, sort_keys=True) )

    try:
        app_requests = _read_event( event )
        logger.debug( "Got app_requests:" )
        logger.debug( json.dumps( app_requests, indent=4, sort_keys=True) )

        _process_app_requests( app_requests )
    except Exception as e:
        logger.error("Unhandled exception caught at top level, bailing: " + str(e) )
        raise e
