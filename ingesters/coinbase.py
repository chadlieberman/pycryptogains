import datetime
from decimal import Decimal
import logging
import sqlalchemy

from apis.coinbase import CoinbasePrimeApi, CoinbaseProApi
from models import Transaction, Account
from database import get_db_session

class TransactionIngester(object):
	def __init__(self, account):
		if account == 'CoinbasePro':
			self._api = CoinbaseProApi
		elif account == 'CoinbasePrime':
			self._api = CoinbasePrimeApi
		else:
			raise Exception('Unrecognized account "%s"' %(opts.account))
		self._account = account
		self._log = logging.getLogger('TransactionIngester')

	def upsert_fill(self, fill, db_session):
		try:
			if fill['side'] == 'sell':
				from_currency, to_currency = fill['product_id'].split('-')
				from_amount = Decimal(fill['size'])
				to_amount = Decimal(fill['size']) * Decimal(fill['price'])
			elif fill['side'] == 'buy':
				to_currency, from_currency = fill['product_id'].split('-')
				from_amount = Decimal(fill['size']) * Decimal(fill['price'])
				to_amount = Decimal(fill['size'])
			else:
				raise Exception('Unrecognized side "%s"' %(fill['side']))
			if fill['usd_volume']:
				usd_volume = fill['usd_volume']
			else:
				if fill['product_id'].find('USD') == -1:
					base, quote = fill['product_id'].split('-')
					usd_price = self._api.get_usd_price(base, fill['created_at'])
					usd_volume = usd_price * Decimal(fill['size'])
				else:
					usd_volume = Decimal(fill['price']) * Decimal(fill['size'])
			txn = Transaction(
				external_id = '%d:%s' %(fill['trade_id'], fill['order_id']),
				from_account = self._account,
				from_currency = from_currency,
				from_amount = from_amount,
				to_account = self._account,
				to_currency = to_currency,
				to_amount = to_amount,
				usd_value = usd_volume,
				fee = fill['fee'],
				transacted_at = fill['created_at'])
			db_session.add(txn)
			db_session.commit()
		# Ignore duplicate key value issues, it's already there
		except sqlalchemy.exc.IntegrityError as e:
			if not str(e).find('duplicate key value violates unique constraint') > -1:
				raise e
			db_session.rollback()
		except Exception as e:
			raise e

	def get_fills(self, backwards_until, afters):
		db_session = get_db_session()
		try:
			for product_id, after in afters.items():
				self._log.info('Ingesting fills for product "%s"' %(product_id))
				for fill in self._api.get_fills(product_id, backwards_until, after):
					self.upsert_fill(fill, db_session)
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
	parser.add_option('-a', '--account',
		type=str,
		help='Name of the account to ingest for {CoinbasePrime, CoinbasePro}')
	parser.add_option('-u', '--backwards-until',
		type=str,
		help='String of datetime to ingest backwards until {2019-01-19T13:59:12.562Z}')
	(opts, args) = parser.parse_args()

	import logging
	import os
	from util import log_setup
	log_setup.setupLogging(opts.log_level)
	log = logging.getLogger('create_trader')

	# Set afters to control what gets ingested
	afters = {
		'ETH-BTC': None,
		'ETH-USD': None,
		'BTC-USD': None,
		'LTC-USD': None,
		'BCH-USD': None,
	}

	ingester = TransactionIngester(opts.account)
	ingester.get_fills(opts.backwards_until, afters)
