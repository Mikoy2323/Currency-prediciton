import src.database as db


def init():
    """Function to initialize database"""
    db.database_init_pipeline()


if __name__ == '__main__':
    init()
