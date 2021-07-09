/**********************************************************************************************************************
Use case: 
          Can be used in migrating data from Vertica to Redshift
Prerequisites: 
          Must move vertica data into S3 bucket connected to the target Redshift database. Be mindful of your bucket path and update the 
          FROM statement of the COPY command to match.
How it works: 
          Run against Vertica and it will output the CREATE TABLEs and the COPY commands to create and populate
          the targeted Vertica tables in Redshift. To be clear, while this query is to be ran against Vertica,
          the output is a set of queries to be ran against Redshift.
NOTE: 
          This query is currently set up to utilize a temporary API key, but there are different authentication options that
          can be found here: 
          https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-access-permissions.html#r_copy-temporary-security-credentials

**********************************************************************************************************************/
SELECT 
'CREATE TABLE ' || LOWER(table_schema) || '.' || LOWER(table_name) ||'
(
    ' || LISTAGG(LOWER(column_name) || ' ' || UPPER(data_type) || '
' USING PARAMETERS max_length = 10000) || '
);

COPY '|| LOWER(table_schema) || '.' || LOWER(table_name) || '
FROM ''s3://<BUCKET/DIR/HERE>' || LOWER(table_schema) || '/' || LOWER(table_name) || '/' || LOWER(table_name) || '.csv''
access_key_id ''<access_key_id_HERE>''
secret_access_key ''secret_access_key_HERE''
SESSION_TOKEN ''<session_token_HERE.''
IGNOREHEADER 1
CSV QUOTE AS ''"''
DELIMITER '',''
TRIMBLANKS
BLANKSASNULL
;'
FROM columns
WHERE LOWER(table_schema || '.' || table_name) IN 
        (
            '<schema.table_1>'
            , '<schema.table_2>'
            , '<schema.table_3>'
        )

GROUP BY table_schema
    ,  table_name
;
