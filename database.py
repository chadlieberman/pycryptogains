from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from models import Base
from config import db_config

engine = create_engine(
	'postgresql://%s:%s@%s/%s' %(
		db_config['username'],
		db_config['password'],
		db_config['host'],
		db_config['dbname']))

def get_db_session():
	db_session = scoped_session(
		sessionmaker(
			autocommit = False,
			autoflush = False,
			bind = engine))
	return db_session

