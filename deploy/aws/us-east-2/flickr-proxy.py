import json
import boto3
import logging


logging_level = logging.DEBUG

logger = logging.getLogger()
logger.setLevel(logging_level)

def attempt_flickr_group_add(event, context):
    logger.debug( "Inside lambda function" )
    logger.debug( json.dumps( event, indent=4, sort_keys=True) )
    #print( json.dumps( event, indent=4, sort_keys=True ) )
