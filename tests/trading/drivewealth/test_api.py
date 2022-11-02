import multiprocessing.dummy
import time
from datetime import datetime, timedelta, timezone

from gainy.utils import db_connect
from gainy.trading.drivewealth import DriveWealthApi, DriveWealthRepository


def _get_token(monkeypatch):
    invocations_count = 0

    def mock_get_auth_token():
        nonlocal invocations_count
        invocations_count += 1
        expires_at = datetime.now(tz=timezone.utc) + timedelta(days=1)

        return {
            "authToken": f"authToken{time.time()}",
            "expiresAt": expires_at.strftime("%Y-%m-%dT%H:%M:%S%z"),
        }

    with db_connect() as db_conn:
        drivewealth_repository = DriveWealthRepository(db_conn)

        api = DriveWealthApi(drivewealth_repository)

        monkeypatch.setattr(api, "get_auth_token", mock_get_auth_token)

        token = api._get_token()

    return token, invocations_count


def test_get_token(monkeypatch):
    threads_count = 5
    with db_connect() as db_conn:
        with db_conn.cursor() as cursor:
            cursor.execute("delete from app.drivewealth_auth_tokens")
            db_conn.commit()

    with multiprocessing.dummy.Pool(threads_count) as pool:
        result = pool.map(_get_token, [monkeypatch] * threads_count)

    tokens = [i[0] for i in result]
    invocations_count = [i[1] for i in result]

    assert len(invocations_count) == threads_count
    assert sum(invocations_count) == 1

    assert len(tokens) == threads_count
    for i in tokens:
        assert i == tokens[0]
