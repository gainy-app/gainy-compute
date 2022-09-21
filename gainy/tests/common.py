from gainy.context_container import ContextContainer


class TestContextContainer(ContextContainer):

    def __exit__(self, exc_type, exc_value, traceback):
        if self._db_conn:
            self._db_conn.rollback()

        super().__exit__(exc_type, exc_value, traceback)
