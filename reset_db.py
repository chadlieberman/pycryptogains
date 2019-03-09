
if __name__ == '__main__':

	from sqlalchemy import create_engine
	from config import db_config
	engine = create_engine(
		'postgresql://%s:%s@%s/%s' %(
			db_config['username'],
			db_config['password'],
			db_config['host'],
			db_config['dbname']))

	# Drop all and create tables
	from models import Base
	Base.metadata.drop_all(engine)
	Base.metadata.create_all(engine)
