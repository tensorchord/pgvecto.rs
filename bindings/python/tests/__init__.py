import os

PORT = os.getenv('DB_PORT', 5432)
HOST = os.getenv('DB_HOST', 'localhost')
USER = os.getenv('DB_USER', 'postgres')
PASS = os.getenv('DB_PASS', 'password')
DB_NAME = os.getenv('DB_NAME', 'postgres')

# Run tests with shell:
#   DB_HOST=localhost DB_USER=postgres DB_PASS=password DB_NAME=postgres python3 -m pytest bindings/python/tests/
URL = 'postgresql+psycopg2://{username}:{password}@{host}:{port}/{db_name}'.format(
    port=PORT,
    host=HOST,
    username=USER,
    password=PASS,
    db_name=DB_NAME,
)
