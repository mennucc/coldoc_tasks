## read https://docs.celeryq.dev/en/stable/userguide/configuration.html

# Broker settings.
broker_url = 'redis://localhost:6379/0'

broker_connection_retry_on_startup = True

## Using the database to store task state and results.
result_backend = 'redis://localhost:6379/0'

# good idea, from
# https://stackoverflow.com/a/69546952/5058564
event_serializer = 'pickle'
task_serializer = 'pickle'
result_serializer = 'pickle'
accept_content = ['application/json', 'application/x-python-serialize']

## other configurations...
#task_annotations = {'tasks.add': {'rate_limit': '10/s'}}


worker_concurrency = 16
