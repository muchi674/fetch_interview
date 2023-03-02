## How to Run
example commands: <br />
docker pull fetchdocker/data-takehome-postgres <br />
docker pull fetchdocker/data-takehome-postgres <br />
docker compose up -d <br />
pip3 install -r requirements.txt <br />
python3 login_etl.py <br />
docker compose down <br />

## Questions
How would you deploy this application in production? <br />
Assuming that Fetch's postgreSQL database in hosted on aws RDS, I would deploy this application in production by the following steps:

1. Create an IAM role to define the aws services and resources this application is allowed to access on Fetch's aws account
2. launch an EC2 instance with the IAM role (wrapped as profile) created in step 1 using launch template
3. allocate an elastic ip on aws VPC to the EC2 instance launched in step 2
4. create a security group on aws VPC and set an inbound rule that whitelists inbound traffic from elastic ip allocated in step 3
5. add the security group created in step 4 to the aws RDS instance that hosts the postgreSQL database that this application interacts with
6. containerize this application with a Dockerfile that defines the runtime environment and a Compose file that defines services that make up this application
7. merge production ready code to master
8. clone git repo on the EC2 instance launched in step 2, cd into project directory, and run docker compose up -d

What other components would you want to add to make this production ready? <br />

1. Right now, inputs to this application is hardcoded. However, in production, said inputs should either be passed in through command line or through HTTP request if this application is made into a small Flask web application launched with, for instance, uWSGI
2. a robust data cleaning component to sanitize raw login data
3. clearly defined exception handling mechanism
4. informative logging mechanism. Where to put the logs? When to send the logs?
5. It is quite unsecure to pass database user passwords around. aws RDS db authentication can also be done through IAM.
6. an independent database role (user) should also be created for the application for better access control

How can this application scale with a growing dataset? <br />

since aws SQS is distributed and that the etl process on each login data is independent of each other, this application/algorithm can also be distributed and parallel. This can be done through Spark and aws EMR.

How can PII be recovered later on? <br />

I masked the PII by simpling rotating the string by the half of its length. Therefore, PII can be simply recovered by reversing the said rotation.

What are the assumptions you made? <br />

1. Aside from removing the dots from the app_version values, I assumed that raw login data all have the correct data type.
2. create_date is relative to the hardware running the application's locale 
