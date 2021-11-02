from arena.core.mysql import Database
from arena.test import tidb


def main():
    test_manager = tidb.test_manager()
    test_manager.run_tests(db=Database(user='root'))


if __name__ == '__main__':
    main()
