import boto3
import sys
import time

athena_client = boto3.client('athena')

def run_query(query_string, results_location, workgroup_name, catalog, database):
    """
    Returns (query_id, succeeded)
    """
    query_id = start_query_execution(query_string, results_location, workgroup_name, catalog, database)
    finished_state = wait_until_query_finished(query_id)
    return query_id, finished_state == 'SUCCEEDED'


def start_query_execution(query_string, results_location, workgroup_name, catalog, database):
    start_query_resp = athena_client.start_query_execution(
        QueryString=query_string,
        ResultConfiguration={
            'OutputLocation': results_location},
        QueryExecutionContext={
            'Catalog': catalog,
            'Database': database,
        },
        WorkGroup=workgroup_name
    )
    return start_query_resp['QueryExecutionId']

def wait_until_query_finished(query_id):
    max_iterations = 100
    sleep_duration = 5
    query_execution, query_execution_status, query_execution_state = '', '', ''
    iterations = 0
    while not query_execution_state == 'FAILED' and not query_execution_state == 'SUCCEEDED' \
            and iterations < max_iterations:
        iterations += 1
        query_execution_response = athena_client.get_query_execution(
            QueryExecutionId=query_id)
        query_execution = query_execution_response['QueryExecution']
        query_execution_status = query_execution['Status']
        query_execution_state = query_execution_status['State']
        time.sleep(sleep_duration)
    if iterations >= max_iterations:
        return 'TIMED OUT'
    if query_execution_state == 'FAILED':
        print(query_execution)
        print(query_execution['Status']['AthenaError'])
    return query_execution_state


'''
Views are created and stored in the awsdatacatalog. So, although we are passing in the catalog/db associated
with the federated catalog, we actually are overriding this for the view creation/selection/deletion itself.
'''
def test_ddl(connector_name, results_location, workgroup_name, catalog, database):
    glue_db = 'default'
    create_view_query = f'''
        CREATE VIEW default.cx_view_{connector_name} AS
            SELECT c.c_customer_sk, c.c_customer_id, c.c_first_name, c.c_last_name, c.c_birth_year, c.c_email_address, ca.ca_address_sk, ca_street_number, ca_street_name
            FROM {catalog}.{database}.customer c JOIN {catalog}.{database}.customer_address ca
            ON c.c_current_addr_sk = ca.ca_address_sk
            WHERE c.c_birth_year = 1989; 
    '''
    select_from_view_query = f'SELECT * FROM cx_view_{connector_name} LIMIT 100;'
    drop_view_query = f'DROP VIEW cx_view_{connector_name};'

    view_query_outcomes = [
        run_query(create_view_query, results_location, workgroup_name, catalog, database),
        run_query(select_from_view_query, results_location, workgroup_name, 'awsdatacatalog', glue_db),
        run_query(drop_view_query, results_location, workgroup_name, 'awsdatacatalog', glue_db)
    ]
    return all(query_outcome[1] for query_outcome in view_query_outcomes)
    

def test_dml(connector_name, results_location, workgroup_name, catalog, database):
    return True

def create_data_source(catalog_name, lambda_arn):
    athena_client.create_data_catalog(Name=catalog_name, Type='LAMBDA', Parameters={'function': lambda_arn})

def delete_data_source(catalog_name):
    athena_client.delete_data_catalog(Name=catalog_name)

def run_queries(connector_name, results_location, lambda_arn):
    '''
    run all our release tests here. we create data catalogs for each connector and delete them after execution.
    this makes the actual queries themselves not need any modifications to append the right JIT lambda catalog name or db name.
    '''

    # db names differ for the datasource, maintain mapping
    db_mapping = {'dynamodb': 'default'}
    
    workgroup_name = 'primary'
    catalog = f'{connector_name}_release_tests_catalog'
    database = db_mapping[connector_name]
    create_data_source(catalog, lambda_arn)
    test_outcomes = [
        test_dml(connector_name, results_location, workgroup_name, catalog, database),
        test_ddl(connector_name, results_location, workgroup_name, catalog, database),
    ]
    delete_data_source(catalog)
    return all(test_outcomes)


if __name__ == '__main__':
    connector_name = sys.argv[1]
    results_location = sys.argv[2]
    lambda_arn = sys.argv[3]
    success = run_queries(connector_name, results_location, lambda_arn)
    if success:
        sys.exit(0)
    else:
        sys.exit(1)

