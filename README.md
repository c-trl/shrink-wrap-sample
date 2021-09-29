# shrink-wrap-sample

Sample of Python collection of API wrappers, database connectors built to standardize analytics scripts. Requires credentials a la boto3 to work.

- T1 API - wrapper to query proprietary API at MediaMath
- Qubole API - wrapper around Qubole SDK to send job execution commands to and retrieve results from Qubole
- Credentials - wrapper around AWS Secrets Manager, used to store and update credentials
- Databases - leverages Credentials to connect to MySQL or PostgreSQL databases
