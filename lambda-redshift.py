import json
import psycopg2
import boto3
import os
 
def lambda_handler(event, context):
    
    print("event collected is {}".format(event))
    #filename =event['Records'][0]['s3']['object']['key']
    # Dictionary mapping s3 file paths to table names 
    # Define the mapping between 53 keys and Redshift tables 
    table_mapping = {
        's3://glue-output-project4/crime scene report/':'crime_scene_report',
        's3://glue-output-project4/driver license/':'drivers_license',
        's3://glue-output-project4/facebook event checking/':'facebook_event_checkin',
        's3://glue-output-project4/get fit now check in/':'get_fit_now_check_in',
        's3://glue-output-project4/get fit now member/':'get_fit_now_member',
        's3://glue-output-project4/income/':'income',
        's3://glue-output-project4/interview/':'interview',
        's3://glue-output-project4/person/':'person'
        }
    dbname = 'dev'
    host = 'redshift-cluster-etl.ciffm7z0fwb1.ap-south-1.redshift.amazonaws.com'
    user = 'awsuser' 
    password = 'Admin#123'
    connection =psycopg2.connect( 
        dbname=dbname, 
        host=host,
        port='5439',
        user=user, 
        password=password
        )
    print('after connection')
    curs = connection.cursor() 
    print('after cursor')
    
    for from_path, tablename in table_mapping.items():
        print ("Processing file from path: {}".format(from_path))
        
        # Truncate the table before copying data 
        truncate_query = "TRUNCATE TABLE {};".format(tablename) 
        curs.execute(truncate_query) 
        #connection.commit() 
        query = "COPY {} FROM '{}' CREDENTIALS 'aws_iam_role=arn:aws:iam::804516561887:role/service-role/AmazonRedshift-CommandsAccessRole-20231103T162946' CSV DATEFORMAT AS 'YYYY-MM-DD' ignoreheader as 1;".format(tablename,from_path)
        print ("query is {}".format(query)) 
        curs.execute(query) 
        connection.commit() 
        print("File imported successfully into table: {}".format(tablename)) 
     
    print('All files imported successfully')
    curs.close() 
    connection.close() 
    print('after connection close')
    
    #e-mail
    
    bClient = boto3.client("ses")

    eSubject = 'AWS Triggering Redshift Event'
    eBody = """
        <br>
        Hey,<br>
        
        Welcome to Project-4 "AWS notification lambda trigger"<br>
        
        We are here to notify you that an event was triggered and all the files are sucessfully loaded into the respective Tables in Redshift<br>
        
    """
    send = {"Subject": {"Data": eSubject}, "Body": {"Html": {"Data": eBody}}}
    result = bClient.send_email(Source= "monishkmgowda01@gmail.com", Destination= {"ToAddresses": ["monishkmgowda01@gmail.com"]}, Message= send)
    print("email sending")
    
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
    
