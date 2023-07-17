import json
import boto3
import time
import os

athena = boto3.client("athena")
s3 = boto3.client("s3")
bucket = os.environ['bucket']

def query_def():
    query = f"select symbol,price_day,closing_price from prices \
                where symbol in \
                (select symbol from prices where price_day in (select distinct price_day from prices order by price_day desc limit 20) group by symbol having count(*) =20) \
                and price_day in (select distinct price_day from prices order by price_day desc limit 20) \
                order by symbol,price_day desc"
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': 'stocks'},
        ResultConfiguration={"OutputLocation": f"s3://{bucket}/queries/"}
    )

    return response["QueryExecutionId"]
    
    
def has_query_succeeded(execution_id):
    state = "RUNNING"
    max_execution = 5

    while max_execution > 0 and state in ["RUNNING", "QUEUED"]:
        max_execution -= 1
        response = athena.get_query_execution(QueryExecutionId=execution_id)
        if (
            "QueryExecution" in response
            and "Status" in response["QueryExecution"]
            and "State" in response["QueryExecution"]["Status"]
        ):
            state = response["QueryExecution"]["Status"]["State"]
            if state == "SUCCEEDED":
                return True

        time.sleep(1)

    return False
    
    

def lambda_handler(event, context):
    
    execution_id = query_def()
    
    query_status = has_query_succeeded(execution_id=execution_id)
    print(f"Query state: {query_status}")
    
    response = athena.get_query_results(QueryExecutionId=execution_id)

    results = response['ResultSet']['Rows']
    while "NextToken" in response:
        #print(response['NextToken'])
        response = athena.get_query_results(QueryExecutionId=execution_id,NextToken=response['NextToken'])
        next_page_results = response['ResultSet']['Rows'] 
        for x in next_page_results[1:]:
            results.append(x)
    
    #results_dump = ""
    #for x in results:
    #    results_dump += json.dumps(x) + "\n"
    #s3.put_object(Body=results_dump, Bucket=bucket, Key="output/results_dump.csv")
    
    output = ""
    prior_symbol = "start"
    for row in results[1:]:
        if str(row["Data"][0]["VarCharValue"]) == prior_symbol:
            price_change = round(((prior_price - float(row["Data"][2]["VarCharValue"])) / float(row["Data"][2]["VarCharValue"]) * 100),1)
            output += prior_symbol + "," + prior_priceday + "," + str(price_change) + "\n"
            
        prior_symbol = row["Data"][0]["VarCharValue"]
        prior_priceday = row["Data"][1]["VarCharValue"]
        prior_price = float(row["Data"][2]["VarCharValue"])
    
    #print(output)
    s3.put_object(Body=output, Bucket=bucket, Key="output/percent_price_change.csv")
    
    return "done"

    