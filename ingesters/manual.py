from models import Transaction, Account
from database import get_db_session
import logging
import sqlalchemy

class TransactionIngester(object):
	def __init__(self, filepath):
		if not os.path.exists(filepath):
			raise Exception('File "%s" does not exist' %(filepath))
		self._filepath = filepath

	def upsert_transaction(self, row, db_session):
		try:
			txn = Transaction(
				external_id = row['external_id'],
				from_account = row['from_account'],
				from_currency = row['from_currency'],
				from_amount = row['from_amount'],
				to_account = row['to_account'],
				to_currency = row['to_currency'],
				to_amount = row['to_amount'],
				usd_value = row['usd_value'],
				fee = row['fee'],
				transacted_at = row['transacted_at'])
			db_session.add(txn)
			db_session.commit()
		# Ignore duplicate key value issues, it's already there
		except sqlalchemy.exc.IntegrityError as e:
			if not str(e).find('duplicate key value violates unique constraint') > -1:
				raise e
			db_session.rollback()
		except Exception as e:
			raise e

	def add_transactions(self):
		import csv
		db_session = get_db_session()
		try:
			with open(self._filepath, 'r') as csvfile:
				reader = csv.DictReader(csvfile)
				for i, row in enumerate(reader):
					self.upsert_transaction(row, db_session)
		except Exception as e:
			db_session.rollback()
			raise e
		finally:
			db_session.close()


if __name__ == '__main__':

	import optparse
	parser = optparse.OptionParser(
		usage='usage: %prog [options]',
		version='%prog 1.0')
	parser.add_option('-L', '--log-level',
		default = 'INFO',
		help = 'log level {DEBUG, INFO, WARNING, ERROR, CRITICAL}. [%default]')
	parser.add_option('-f', '--csv-file',
		type=str,
		help='Path to the CSV file to upload')
	(opts, args) = parser.parse_args()

	import logging
	import os
	from util import log_setup
	log_setup.setupLogging(opts.log_level)
	log = logging.getLogger('main')

	ingester = TransactionIngester(opts.csv_file)
	ingester.add_transactions()
