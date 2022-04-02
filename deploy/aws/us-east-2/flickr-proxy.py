import json
import boto3
import logging


logging_level = logging.DEBUG

logger = logging.getLogger()
logger.setLevel(logging_level)



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


def attempt_flickr_group_add(event, context):
    logger.debug( json.dumps( event, indent=4, sort_keys=True) )
    #print( json.dumps( event, indent=4, sort_keys=True ) )

    try:
        app_requests = _read_event( event )
        logger.debug( "Got app_requests:" )
        logger.debug( json.dumps( app_requests, indent=4, sort_keys=True) )
    except:
        logger.warn("Unhandled exception thrown, bailing")
