# shrink-wrap-sample

Sample of Python collection of API wrappers, database connectors built to standardize analytics scripts. Requires credentials a la boto3 to work.

- T1 API - wrapper to query proprietary API at MediaMath
  - API Docs: https://apidocs.mediamath.com/
- Qubole API - wrapper around Qubole SDK to send job execution commands to and retrieve results from Qubole
  - API Docs: https://docs.qubole.com/en/latest/rest-api/index.html
- Credentials - wrapper around AWS DynamoDB, used to store and update credentials
- Databases - leverages Credentials to connect to MySQL or PostgreSQL databases
