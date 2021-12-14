import tempfile

from data_access.repository import DatabaseTickerRepository
import importlib

def test_repo():

    repo = DatabaseTickerRepository(
        db_host="localhost",
        db_port=5432,
        db_user="postgres",
        db_password="postgrespassword",
        db_name="postgres"
    )

    print(repo.load_manual_ticker_industries())