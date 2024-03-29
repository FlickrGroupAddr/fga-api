import json
import boto3
import logging
import flickrapi
import flickrapi.auth
import datetime
import uuid
import pprint
import os
import psycopg2


logging_level = logging.DEBUG

logger = logging.getLogger()
logger.setLevel(logging_level)

ssm = boto3.client('ssm', region_name='us-east-2' )



def _valid_app_request( app_request ):
    # Make sure it's a list, and that every entry has the proper format
    if isinstance( app_request, list ) is False:
        return False

    required_keys = (
        "user_submitted_request_id",
        "user_cognito_id",
        "flickr_picture_id",
        "flickr_group_id"
    )

    for curr_entry in app_request:
        if all( key in curr_entry for key in required_keys ) is False:
            return False

    # We know we're good to go
    return True



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
        user_flickr_auth_info['oauth_token'],
        user_flickr_auth_info['oauth_token_secret'],
        api_access_level,
        user_flickr_auth_info['fullname'],
        user_flickr_auth_info['username'],
        user_flickr_auth_info['user_nsid'])

    flickrapi_handle = flickrapi.FlickrAPI(app_flickr_api_key_info['api_key'],
                                           app_flickr_api_key_info['api_key_secret'],
                                           token=flickrapi_user_token,
                                           store_token=False,
                                           format='parsed-json')

    return flickrapi_handle


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
            logger.warn( f"Exception thrown when getting group memberships for user: {str(e)}" )

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
            logger.warn( f"Exception thrown when trying to get Flickr groups for pic {pic_id}: {str(e)}" )

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


def _attempt_flickr_add( curr_user_request, flickrapi_handle, db_cursor, group_user_throttling_state ):

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

        # Note throttling so we don't try this (group + user) combo again this run
        if attempt_status == 'defer_group_throttled_for_user':
            group_user_throttling_state[ curr_user_request['flickr_group_id'] ] = { 
                curr_user_request['user_cognito_id']: None 
            }
            logger.info( f"Noted that group {curr_user_request['flickr_group_id']} has throttled user " + 
                curr_user_request['user_cognito_id'] )



    logger.info( f"Final Flickr operation status: {attempt_status}" )

    timestamp_end           = datetime.datetime.now( datetime.timezone.utc )

    sql_command = """
        INSERT INTO group_add_attempts ( uuid_pk, submitted_request_fk, attempt_started,
            attempt_completed, final_status )
        VALUES ( %s, %s, %s, %s, %s );
        """

    add_attempt_guid = str( uuid.uuid4() )

    sql_params = (
        add_attempt_guid,
        curr_user_request['user_submitted_request_id'],
        timestamp_start.isoformat(),
        timestamp_end.isoformat(),
        attempt_status
    )

    db_cursor.execute( sql_command, sql_params )



def _get_db_handle( ):
    pgsql_creds = _get_postgresql_creds()

    return psycopg2.connect(
        host        = pgsql_creds['db_host'],
        user        = pgsql_creds['db_user'],
        password    = pgsql_creds['db_passwd'],
        database    = pgsql_creds['database_name'] )



def _process_app_requests( app_requests ):
    flickr_creds_app = _get_flickr_app_creds()
    logger.debug( "Successfully retrieved Flickr user and app creds from Parameter Store" )

    with _get_db_handle() as db_handle:
        with db_handle.cursor() as db_cursor:

            group_user_throttling_state = {} 

            for curr_request_batch in app_requests:
                logger.info( "Processing request batch:\n" + json.dumps(curr_request_batch, indent=4, sort_keys=True) )

                for curr_request in curr_request_batch:

                    # if this user has been throttled by the current group, skip them
                    curr_group  = curr_request[ 'flickr_group_id' ]
                    curr_user   = curr_request[ 'user_cognito_id' ]
                    if curr_group in group_user_throttling_state and curr_user in group_user_throttling_state[curr_group]:
                        logger.info( f"Skipping add attempt for user {curr_user} to group {curr_group}, they are throttled for the day" )
                        continue

                    flickr_creds_user = _get_flickr_user_creds( curr_user )
                    if not flickr_creds_user:
                        logger.warn( f"Could not find flickr creds for Cognito user ID \"{curr_user}\", bailing" )
                        continue

                    flickrapi_handle = _create_flickr_api_handle( flickr_creds_app, flickr_creds_user )
                    logger.debug( "Successfully created Flickr API handle with app & user creds" )

                    _attempt_flickr_add( curr_request, flickrapi_handle, db_cursor, group_user_throttling_state )

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
