## read https://docs.celeryq.dev/en/stable/userguide/configuration.html

# Broker settings.
broker_url = 'redis://localhost:6379/0'

## Using the database to store task state and results.
esult_backend = 'redis://localhost:6379/0'

# good idea, from
# https://stackoverflow.com/a/69546952/5058564
event_serializer = 'pickle'
task_serializer = 'pickle'
result_serializer = 'pickle'
accept_content = ['application/json', 'application/x-python-serialize']

## other configurations...
#task_annotations = {'tasks.add': {'rate_limit': '10/s'}}


worker_concurrency = 16
