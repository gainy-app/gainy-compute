# gainy-compute
Everything related to data computations and ML models in Python 

# Development of new optimization
- ticker_chooser -- if used default add collection ID to init.py
- if need to change optimization parameters -- add if statement in jobs/optimize_collections.py


# To run localy
-  Add data to fixtures 
- Run apt update && apt install -y postgresql-client to install postgress
- PGPASSWORD=$PG_PASSWORD psql -h $PG_HOST -p $PG_PORT -U $PG_USERNAME $PG_DBNAME to connect to DB
- poetry run gainy_optimize_collections to run optimization 'poetry run gainy_optimize_collections -h' for params