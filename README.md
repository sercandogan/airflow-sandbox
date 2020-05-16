# airflow-sandbox



#### Create data warehouse:
```bash
docker-compose exec postgres bash
```
```bash
psql -U airflow
```
```bash
 CREATE DATABASE dw OWNER airflow;
```

#### Airflow create user
```bash
usage: airflow create_user [-h] [-r ROLE] [-u USERNAME] [-e EMAIL]
                           [-f FIRSTNAME] [-l LASTNAME] [-p PASSWORD]
                           [--use_random_password]

optional arguments:
  -h, --help            show this help message and exit
  -r ROLE, --role ROLE  Role of the user. Existing roles include Admin, User,
                        Op, Viewer, and Public
  -u USERNAME, --username USERNAME
                        Username of the user
  -e EMAIL, --email EMAIL
                        Email of the user
  -f FIRSTNAME, --firstname FIRSTNAME
                        First name of the user
  -l LASTNAME, --lastname LASTNAME
                        Last name of the user
  -p PASSWORD, --password PASSWORD
                        Password of the user
  --use_random_password
                        Do not prompt for password. Use random string instead

```