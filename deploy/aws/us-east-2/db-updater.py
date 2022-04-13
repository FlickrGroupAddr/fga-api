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


def _record_attempt( db_cursor, lambda_event ):
    if "Records" in lambda_event:
        for curr_record in lambda_event["Records"]:
            if 'body' in curr_record:
                message_body = json.loads( curr_record['body'] )
                sql_command = """
                    INSERT INTO group_add_attempts ( uuid_pk, submitted_request_fk, attempt_started,
                        attempt_completed, final_status )
                    VALUES ( %s, %s, %s, %s, %s );
                """

                add_attempt_guid = str( uuid.uuid4() )

                sql_params = (
                    add_attempt_guid,
                    message_body['user_submitted_request_id'],
                    message_body['timestamp_attempt_started'],
                    message_body['timestamp_attempt_ended'],
                    message_body['final_status'],
                )

                db_cursor.execute( sql_command, sql_params )


def update_group_add_attempt(event, context):
    logger.debug( json.dumps( event, indent=4, sort_keys=True) )

    try:
        with _get_db_handle() as db_handle:
            with db_handle.cursor() as db_cursor:
                _record_attempt( db_cursor, event )

        
    except Exception as e:
        # With SQS as a trigger, never ever fail, just log as a critical
        #       I've had enough $7,000 AWS bills, thanks
        logger.critical("Unhandled exception caught at top level, bailing: " + str(e) )

    logger.debug( "Exiting lambda" )
