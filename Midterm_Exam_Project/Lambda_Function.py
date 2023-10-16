import json
import boto3
from datetime import datetime, timedelta
import subprocess
from send_email import send_email


def lambda_handler(event, context):
    s3_file_list = []
    
    s3_client = boto3.client('s3')
    for obj in s3_client.list_objects_v2(Bucket='de-midterm-raw')['Contents']:
        s3_file_list.append(obj['Key'])
    print("s3_file_list", s3_file_list)
    
    datestr = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    required_file_list = [f'calendar_{datestr}.csv', f'inventory_{datestr}.csv', f'product_{datestr}.csv', f'sales_{datestr}.csv', f'store_{datestr}.csv']
    print("required_file_list:", required_file_list)
    

    ###scan S3 bucket
    if set(required_file_list).issubset(set(s3_file_list)):
        s3_file_url = ['s3://' + 'de-midterm-raw/' + a for a in s3_file_list]
        # print("s3_file_url:", s3_file_url)
        table_name = [a[:-15] for a in s3_file_list]
        print("table_name:", table_name)

        data = json.dumps({'conf':{a:b for a, b in zip(table_name, s3_file_url)}})
        print("data:", data)

# send signal to Airflow    
        endpoint= 'http://18.218.110.158:8080/api/v1/dags/midterm_dag/dagRuns'
    
        subprocess.run([
            'curl', 
            '-X', 
            'POST', 
            endpoint, 
            '-H',
            'Content-Type: application/json',
            '--user',
            'airflow:airflow',
            '--data', 
            data])
       
        print('File are send to Airflow')
    else:
        send_email()
