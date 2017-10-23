import os

environment = os.environ.get('ENVIRONMENT', 'staging')

if environment == 'production':
    targets = ['camus']
else:
    targets = ['camus-staging']
