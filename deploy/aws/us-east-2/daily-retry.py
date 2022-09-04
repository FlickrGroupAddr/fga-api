import json
import boto3
import logging
import datetime
import psycopg2
import uuid
import os


logging_level = logging.INFO

logger = logging.getLogger()
logger.setLevel(logging_level)

ssm = boto3.client('ssm', region_name='us-east-2' )
sns = boto3.client('sns', region_name='us-east-2' )


def _read_value_from_ssm( path_string ):
    ssm_response = None

    try:
        ssm_response = json.loads(
            ssm.get_parameter(Name=path_string)['Parameter']['Value'] )

    except:
        logger.error(f"Exception thrown when trying to read SSM data at path {path_string}" )

    return ssm_response



def _get_pgsql_creds():
    return _read_value_from_ssm( "/flickrgroupaddr/resources/pgsql/creds" )



def _get_db_handle( ):
    pgsql_creds = _get_pgsql_creds()

    return psycopg2.connect(
        host        = pgsql_creds['db_host'],
        user        = pgsql_creds['db_user'],
        password    = pgsql_creds['db_passwd'],
        database    = pgsql_creds['database_name'] )


def _do_sns_notify( ordered_daily_batch_of_requests ):

    # let's see if the attempt to store the ARN of the generated SNS topic by serverless in env var worked
    sns_topic_arn = os.getenv( 'SNS_TOPIC_ARN' )
    if sns_topic_arn:
        logger.debug( f"SNS Topic ARN was found in env vars: {sns_topic_arn}, sending notification" )
        try:
            logger.debug( "SNS notification text" )
            sns_notification_text = json.dumps( ordered_daily_batch_of_requests, indent=4, sort_keys=True )
            logger.debug( sns_notification_text )

            sns.publish(
                TopicArn    = sns_topic_arn,
                Message     = sns_notification_text,
            )

            logger.info( "Successfully published notification of daily retry requests to SNS" )
        except Exception as e:
            if logging_level == logging.DEBUG:
                raise e
            else:
                logger.warn( f"Exception thrown when trying to publish to SNS topic: {str(e)}" )
    else:
        logger.warn( "Could not find SNS topic ARN in env var, cannot post to SNS" )



def _get_ordered_retry_attempts( db_cursor ):
    # Get list of all requests that DO NOT have a permanent status
    #   List will be returned in the order we should attempt adds:
    #       for each group:
    #           chronological (oldest to newest) pic->group add requests for that user into the current group 
    sql_command = """
        SELECT          uuid_pk, flickr_user_cognito_id, picture_flickr_id, flickr_group_id
        FROM            submitted_requests
        WHERE           uuid_pk NOT IN ( 
            SELECT      submitted_request_fk 
            FROM        group_add_attempts 
            WHERE       final_status LIKE 'permstatus_%%' 
        )
        ORDER BY        flickr_group_id, flickr_user_cognito_id, request_datetime;
    """
    db_cursor.execute( sql_command )

    attempt_list = []
    for curr_result_row in db_cursor.fetchall():
        full_request_entry = {
            "user_cognito_id"               : curr_result_row[1],
            "flickr_picture_id"             : curr_result_row[2],
            "flickr_group_id"               : curr_result_row[3],
            "user_submitted_request_id"     : curr_result_row[0],
        }

        attempt_list.append( full_request_entry )

    # We don't need to do any filtering on date; this daily script is run a few seconds after each new UTC day ticks over, 
    #   so all these are valid requests with a chance of succeeding
    return attempt_list


def daily_retry(event, context):
    try:
        with _get_db_handle() as db_handle:
            with db_handle.cursor() as db_cursor:
                ordered_requests_to_retry = _get_ordered_retry_attempts( db_cursor )

                # If there are any request to retry, send the batch of entries for this daily run in one message
                if len( ordered_requests_to_retry ) > 0:
                    _do_sns_notify( ordered_requests_to_retry )
                else:
                    logger.info( "All requests have been added, nothing to retry" )

    except Exception as e:
        if logging_level == logging.DEBUG:
            logger.critical( "Unhandled exception at top level: " + str(e) )
            raise e
        else:
            logger.critical("Unhandled exception caught at top level, bailing: " + str(e) )

    logger.debug( "Exiting lambda" )
