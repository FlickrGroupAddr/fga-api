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


def _do_sns_notify( user_cognito_id, flickr_photo_id, flickr_group_id, request_guid ):
    logger.debug( "Starting SNS notification due to successful DB insert" )

    # let's see if the attempt to store the ARN of the generated SNS topic by serverless in env var worked
    sns_topic_arn = os.getenv( 'SNS_TOPIC_ARN' )
    if sns_topic_arn:
        logger.debug( f"SNS Topic ARN was found in env vars: {sns_topic_arn}, sending notification" )
        try:
            sns_notification = {
                "user_submitted_request_id"     : request_guid,
                "user_cognito_id"               : user_cognito_id,
                "flickr_picture_id"             : flickr_photo_id,
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



def _get_retry_attempts( db_cursor ):
    sql_command = """
        SELECT          submitted_requests.uuid_pk AS request_guid,
                        flickr_user_cognito_id,
                        picture_flickr_id,
                        flickr_group_id,
                        attempt_completed,
                        final_status
        FROM            submitted_requests
        LEFT JOIN       group_add_attempts
        ON              submitted_requests.uuid_pk = group_add_attempts.submitted_request_fk
        ORDER BY        request_datetime, group_add_attempts.attempt_completed DESC;
    """

    db_cursor.execute( sql_command )

    last_request_guid = None

    curr_utc_date = current_timestamp = datetime.datetime.now(
        datetime.timezone.utc).replace(microsecond=0).date()

    retry_attempts = []

    for curr_result_row in db_cursor.fetchall():
        curr_request_guid = curr_result_row[0]
        if curr_request_guid != last_request_guid:
            # Have new request guid.  First row is newest attempt. Make sure it was not
            #   the current UTC day
            if curr_result_row[4] is not None:
                most_recent_attempt_date = curr_result_row[4].date()

                if most_recent_attempt_date != curr_utc_date:
                    # Find out if most recent status was permanent
                    most_recent_status = curr_result_row[5]
                    if most_recent_status is not None and most_recent_status.startswith("permstatus_") is False:
                        #print( "Need to retry this row: " + json.dumps(curr_result_row, default=str) )
                        new_retry_attempt = {
                            "user_cognito_id"       : curr_result_row[1],
                            "flickr_photo_id"       : curr_result_row[2],
                            "flickr_group_id"       : curr_result_row[3],
                            "request_guid"          : curr_result_row[0],
                        }

                        retry_attempts.append( new_retry_attempt )

        last_request_guid = curr_request_guid

    return retry_attempts


def daily_retry(event, context):
    try:
        with _get_db_handle() as db_handle:
            with db_handle.cursor() as db_cursor:
                requests_to_retry = _get_retry_attempts( db_cursor )

        # Attempt the daily retries
        for curr_retry in requests_to_retry:
            logger.info( "Retrying:\n" + json.dumps(
                curr_retry, sort_keys=True, indent=4, default=str) )

            _do_sns_notify(
                curr_retry['user_cognito_id'],
                curr_retry['flickr_photo_id'],
                curr_retry['flickr_group_id'],
                curr_retry['request_guid'] )



    except Exception as e:
        if logging_level == logging.DEBUG:
            logger.critical( "Unhandled exception at top level: " + str(e) )
            raise e
        else:
            logger.critical("Unhandled exception caught at top level, bailing: " + str(e) )

    logger.debug( "Exiting lambda" )
