import json
import boto3
 
def lambda_handler(event, context):
    try:
        #print(event)
        message=event['Records'][0]['Sns']['Message']
        print(message)
        filename=json.loads(message)['Records'][0]['s3']['object']['key']
        print(filename)
        # TODO implement
        client=boto3.client("glue")
        client.start_job_run(
            JobName='driver license'
            )
        client.start_job_run(
            JobName='income'
            )
        client.start_job_run(
            JobName='person'
            )
        client.start_job_run(
            JobName='crime scene report'
            )
        client.start_job_run(
            JobName='facebook event checking'
            )
        client.start_job_run(
            JobName='interview'
            )
        client.start_job_run(
            JobName='get fit now member'
            )
        client.start_job_run(
            JobName='get fit now check in'
            )
        print("sucessful") 
        
        #e-mail
        
        #for e in event["Records"]:
        #    bucketName = e["s3"]["bucket"]["name"]
        #    objectName = e["s3"]["object"]["key"]
        #    eventName = e["eventName"]
        
        bClient = boto3.client("ses")
        
        eSubject = 'AWS Triggering Glue Job Event'
        
        eBody = """
            <br>
            Hey,<br>
            
            Welcome to Project-4 "AWS S3 notification lambda trigger"<br>
            
            We are here to notify you that an event was triggered.<br>
            <br>
            Object name : {}
            <br>
        """.format(filename)
        
        send = {"Subject": {"Data": eSubject}, "Body": {"Html": {"Data": eBody}}}
        result = bClient.send_email(Source= "monishkmgowda01@gmail.com", Destination= {"ToAddresses": ["monishkmgowda01@gmail.com"]}, Message= send)
        
    except Exception as e:
        ses = boto3.client('ses')
        # Send error- email notification
        subject = "Lambda Error Notification while running Glue Job"
        error_message = f"An error occurred in the Lambda function: {str(e)}"
        recipient_email = "monishkmgowda01@gmail.com"  
        sender_email = "monishkmgowda01@gmail.com"

        # Send the email
        response = ses.send_email(
            Source=sender_email,
            Destination={
                'ToAddresses': [recipient_email]
            },
            Message={
                'Subject': {'Data': subject},
                'Body': {'Text': {'Data': error_message}}
            }
        )

        return {
            'statusCode': 500,
            'body': json.dumps(response)# failure alert
        }    
        
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
        