import datetime
from decimal import Decimal
import logging
import json
import os
import pickle
import traceback
import sys

from apis.coinbase import CoinbasePrimeApi
from models import CURRENCIES, Transaction, Account, Currency
from database import get_db_session

class OpenTransaction(object):
	def __init__(self,
		transaction_id,
		currency,
		original_amount,
		price,
		fee,
		transacted_at):
		self._transaction_id = transaction_id
		self._currency = currency
		self._original_amount = Decimal(original_amount)
		self._price = Decimal(price) # per coin
		self._fee = Decimal(fee) # per coin
		self._transacted_at = transacted_at
		self._remaining_amount = self._original_amount

	def __str__(self):
		return self.__repr__()

	def __repr__(self):
		record = "\n[OpenTxn\n"
		record += "  transaction_id: %s\n" %(self._transaction_id)
		record += "  currency: %s\n" %(self._currency)
		record += "  original_amount: %s\n" %(self._original_amount)
		record += "  price: %s\n" %(self._price)
		record += "  fee: %s\n" %(self._fee)
		record += "  transacted_at: %s\n" %(self._transacted_at)
		record += "  remaining_amount: %s\n" %(self._remaining_amount)
		record += "]\n"
		return record

class CapGainEvent(object):
	def __init__(self,
		buy_txn_id,
		sell_txn_id,
		currency,
		qty,
		cost_basis,
		purchased_at,
		proceeds,
		sold_at,
		gain,
		is_short_term = True):
		self._buy_txn_id = buy_txn_id
		self._sell_txn_id = sell_txn_id
		self._currency = currency
		self._qty = qty
		self._cost_basis = cost_basis
		self._purchased_at = purchased_at
		self._proceeds = proceeds
		self._sold_at = sold_at
		self._gain = gain
		self._is_short_term = is_short_term

	def __str__(self):
		return self.__repr__()

	def __repr__(self):
		record = "[CapGainEvent\n"
		record += "  buy_txn_id: %d\n" %(self._buy_txn_id)
		record += "  sell_txn_id: %d\n" %(self._sell_txn_id)
		record += "  currency: %s\n" %(self._currency)
		record += "  qty: %1.12f\n" %(self._qty)
		record += "  purchased_at: %s\n" %(self._purchased_at)
		record += "  sold_at: %s\n" %(self._sold_at)
		record += "  gain: %1.12f\n" %(self._gain)
		record += "  is_short_term: %s\n" %('True' if self._is_short_term else 'False')
		record += "]\n"
		return record

class VirtualWallet(object):
	def __init__(self, currency, time):
		self._currency = currency
		self._time = time
		self._open_txns = list()

	def process(self, transaction):
		if transaction.to_currency == transaction.from_currency and (transaction.to_account != Account.External and transaction.from_account != Account.External):
			print(transaction)
			raise Exception('Transactions with the same to/from currency do not incur cap gains')
		cap_gain_events = list()
		is_bought = (transaction.from_account == transaction.to_account and transaction.to_currency == self._currency) \
				 or (transaction.from_account == Account.External and transaction.to_currency == self._currency)
		is_sold = (transaction.from_account == transaction.to_account and transaction.from_currency == self._currency) \
			   or (transaction.to_account == Account.External and transaction.from_currency == self._currency)

		if is_bought:
			transaction_price = Decimal(transaction.usd_value) / Decimal(transaction.to_amount)
			transaction_fee = Decimal(transaction.fee) / Decimal(transaction.to_amount)
			# Add to open transactions
			self._open_txns.append(
				OpenTransaction(
					transaction.id,
					self._currency,
					transaction.to_amount,
					transaction_price, # per coin
					transaction_fee, # per coin
					transaction.transacted_at))

		elif is_sold:
			# Match open transactions from earliest first
			txn_qty = Decimal(transaction.from_amount)
			txn_price = Decimal(transaction.usd_value) / txn_qty
			txn_fee = Decimal(transaction.fee) / txn_qty
			remove_up_to = 0
			for index, open_txn in enumerate(self._open_txns):
				match_amount = min(open_txn._remaining_amount, txn_qty)
				txn_qty -= match_amount
				open_txn._remaining_amount -= match_amount
				gain = match_amount * (txn_price - open_txn._price - txn_fee - open_txn._fee)
				is_short_term = transaction.transacted_at - open_txn._transacted_at < datetime.timedelta(days=365)
				cap_gain_events.append(
					CapGainEvent(
						open_txn._transaction_id,
						transaction.id,
						self._currency,
						match_amount, # qty
						match_amount * (open_txn._price + txn_fee + open_txn._fee), # cost_basis
						open_txn._transacted_at,
						match_amount * txn_price, # proceeds
						transaction.transacted_at,
						gain,
						is_short_term))
				if open_txn._remaining_amount == 0.0:
					remove_up_to = index + 1
				if txn_qty == 0.0:
					break
			# Remove open transactions with 0 remaining amount
			self._open_txns = self._open_txns[remove_up_to:]
			if txn_qty != 0.0:
				raise Exception('Insufficient open transactions to match, transaction_id=%d' %(transaction.id))

		else:
			raise Exception('Transaction does not involve "%s"' %(self._currency))
		return cap_gain_events

	def metrics(self):
		outstanding_qty = Decimal(0.0)
		total_cost = Decimal(0.0)
		for open_txn in self._open_txns:
			outstanding_qty += open_txn._remaining_amount
			total_cost += open_txn._remaining_amount * open_txn._price
		if outstanding_qty > 0.0:
			avg_cost = total_cost / outstanding_qty
		else:
			avg_cost = Decimal(0.0)
		return outstanding_qty, avg_cost

	def report(self, mark_time):
		outstanding_qty, avg_cost = self.metrics()
		if outstanding_qty > 0.0:
			price = CoinbasePrimeApi.get_usd_price(CURRENCIES[self._currency.value], mark_time)
			return {
				'last_transacted_time': self._time.isoformat(),
				'mark_time': mark_time.isoformat(),
				'market_price': price,
				'outstanding_qty': outstanding_qty,
				'avg_cost': avg_cost,
				'unrealized_gains': outstanding_qty * (price - avg_cost)
			}
		else:
			return {
				'last_transacted_time': self._time.isoformat(),
				'mark_time': mark_time.isoformat(),
				'outstanding_qty': outstanding_qty,
				'avg_cost': avg_cost,
				'market_price': Decimal(0.0),
				'unrealized_gains': Decimal(0.0)
			}

class Portfolio(object):
	DATETIME_FORMAT = '%Y%m%d.%H%M%S%f'
	def __init__(self, name, time):
		self._name = name
		self._time = time
		self._wallets = dict()

	@staticmethod
	def time2str(time):
		return time.strftime(Portfolio.DATETIME_FORMAT)

	@staticmethod
	def file2time(filepath):
		filename = os.path.basename(filepath)
		string = filename[10:-4] # pulls part in "portfolio.(?).pkl"
		return datetime.strptime(string, Portfolio.DATETIME_FORMAT)

	@staticmethod
	def time2file(time):
		return os.path.join('portfolio.%s.pkl' %(Portfolio.time2str(time)))

	@staticmethod
	def load(name, time):
		filepath = os.path.join('data', name, Portfolio.time2file(time))
		if not os.path.exists(filepath):
			raise Exception('No portfolio file exists at "%s"' %(filepath))
		with open(filepath, 'rb') as pickle_file:
			data = pickle.load(pickle_file)
		return data

	def save(self):
		filepath = os.path.join(account_dir, Portfolio.time2file(self._time))
		with open(filepath, 'wb') as pickle_file:
			pickle.dump(self, pickle_file)

	def process(self, transaction):
		# Determine the relevant currencies (excludes USD)
		relevant_currencies = [transaction.to_currency, transaction.from_currency]
		relevant_currencies = filter(lambda x: x != Currency.USD, relevant_currencies)
		relevant_currencies = set(relevant_currencies)
		cap_gain_events = list()
		for currency in relevant_currencies:
			# Create virtual wallet if it doesn't exist yet
			if currency not in self._wallets:
				self._wallets[currency] = VirtualWallet(currency, transaction.transacted_at)
			# Process the transaction
			cap_gain_events += self._wallets[currency].process(transaction)
		self._time = transaction.transacted_at
		return cap_gain_events

	def mark(self):
		report = dict()
		report['wallets'] = dict()
		for currency, wallet in self._wallets.iteritems():
			report['wallets'][currency] = wallet.report(self._time)
		total_unrealized_gains = reduce(
			lambda x, y: x + y['unrealized_gains'],
			report['wallets'].values(),
			Decimal(0.0))
		report['summary'] = {
			'time': self._time.isoformat(),
			'total_unrealized_gains': total_unrealized_gains
		}
		return report

class CapGainsAggregator(object):
	class TermAggr(object):
		def __init__(self):
			self._gain = Decimal(0.0)
			self._purchased_range = [datetime.datetime(1970,1,1), datetime.datetime(1970,1,1)]
			self._sold_range = [datetime.datetime(1970,1,1), datetime.datetime(1970,1,1)]
			self._total_qty = Decimal(0.0)
			self._total_cost_basis = Decimal(0.0)
			self._total_proceeds = Decimal(0.0)

		def add_event(self, event):
			self._gain += event._gain
			self._total_qty += event._qty
			self._total_cost_basis += event._cost_basis
			self._total_proceeds += event._proceeds
			self._purchased_range = [
				min(self._purchased_range[0], event._purchased_at),
				max(self._purchased_range[1], event._purchased_at)]
			self._sold_range = [
				min(self._sold_range[0], event._sold_at),
				max(self._sold_range[1], event._sold_at)]

	def __init__(self):
		self._short = CapGainsAggregator.TermAggr()
		self._long = CapGainsAggregator.TermAggr()

	def add_event(self, event):
		if event._is_short_term:
			self._short.add_event(event)
		else: # long term
			self._long.add_event(event)

class CapFifoQueue(object):
	def __init__(self,
		portfolio,
		end_time = None):
		self._log = logging.getLogger('CapFifoQueue')
		self._portfolio = portfolio
		self._start_time = portfolio._time
		self._end_time = end_time
		self._cap_gains_aggrs = dict()
		self._num_txns_processed = 0
		self._time = self._start_time

	def process(self, transaction):
		if transaction.transacted_at > self._end_time:
			self._log.warn('Transaction is after end_time')
			return
		cap_gain_events = self._portfolio.process(transaction)
		self._time = transaction.transacted_at
		for cap_gain_event in cap_gain_events:
			if cap_gain_event._currency not in self._cap_gains_aggrs:
				self._cap_gains_aggrs[cap_gain_event._currency] = CapGainsAggregator()
			self._cap_gains_aggrs[cap_gain_event._currency].add_event(cap_gain_event)

	def get_transactions(self):
		if self._portfolio._name == 'business':
			transactions = self._db_session.query(Transaction)\
				.filter(Transaction.from_account == Account.CoinbasePrime)\
				.filter(Transaction.to_account == Account.CoinbasePrime)\
				.filter(Transaction.transacted_at >= self._start_time)\
				.filter(Transaction.transacted_at < self._end_time)\
				.order_by(Transaction.transacted_at.asc())
			self._log.info('Found %d transactions' %(transactions.count()))
		elif self._portfolio._name == 'personal':
			transactions = self._db_session.query(Transaction)\
				.filter(Transaction.from_account != Account.CoinbasePrime)\
				.filter(Transaction.to_account != Account.CoinbasePrime)\
				.filter(Transaction.transacted_at >= self._start_time)\
				.filter(Transaction.transacted_at < self._end_time)\
				.order_by(Transaction.transacted_at.asc())
			self._log.info('Found %d transactions' %(transactions.count()))
		else:
			raise Exception('Unrecognized portfolio name "%s"' %(self._portfolio._name))
		return transactions

	def process_transactions(self):
		self._log.info('process_transactions')
		self._log.info('start_time = %s', self._start_time.isoformat())
		self._log.info('end_time = %s', self._end_time.isoformat())
		self._db_session = get_db_session()
		try:
			transactions = self.get_transactions()
			for transaction in transactions:
				self.process(transaction)
				self._num_txns_processed += 1
			self._portfolio._time = self._end_time
		except Exception as e:
			print(traceback.format_exc())
			raise e
		finally:
			self._db_session.close()

class CapGains(object):

	@staticmethod
	def report(name, start_time, end_time):
		start_portfolio = Portfolio.load(name, start_time)
		fifo_queue = CapFifoQueue(
			portfolio = start_portfolio,
			end_time = end_time)
		fifo_queue.process_transactions()
		fifo_queue._portfolio.save()
		short_details = [
			{
				'currency': CURRENCIES[key.value],
				'total_qty': aggr._short._total_qty,
				'total_cost_basis': aggr._short._total_cost_basis,
				'total_proceeds': aggr._short._total_proceeds,
				'gain': aggr._short._gain
			}
		for key, aggr in fifo_queue._cap_gains_aggrs.iteritems()]
		short = {
			'gain': reduce(lambda x, y: x + y['gain'], short_details, Decimal(0.0)),
			'details': short_details
		}
		long_details = [
			{
				'currency': CURRENCIES[key.value],
				'total_qty': aggr._long._total_qty,
				'total_cost_basis': aggr._long._total_cost_basis,
				'total_proceeds': aggr._long._total_proceeds,
				'gain': aggr._long._gain
			}
		for key, aggr in fifo_queue._cap_gains_aggrs.iteritems()]
		long = {
			'gain': reduce(lambda x, y: x + y['gain'], long_details, Decimal(0.0)),
			'details': long_details
		}
		return {
			'start_time': start_time.isoformat(),
			'end_time': end_time.isoformat(),
			'short_term': short,
			'long_term': long,
			'unrealized_gains': fifo_queue._portfolio.mark(),
			'num_txns_processed': fifo_queue._num_txns_processed
		}

if __name__ == '__main__':

	import optparse
	parser = optparse.OptionParser(
		usage='usage: %prog [options]',
		version='%prog 1.0')
	parser.add_option('-L', '--log-level',
		default = 'INFO',
		help = 'log level {DEBUG, INFO, WARNING, ERROR, CRITICAL}. [%default]')
	parser.add_option('-t', '--type',
		type=str,
		help='Name of the account type {business, personal}')
	parser.add_option('-s', '--start-time',
		type=str,
		help='String of datetime to start {2019-01-19T13:59:12.562Z}')
	parser.add_option('-e', '--end-time',
		type=str,
		help='String of datetime to end {2019-01-19T13:59:12.562Z}')
	(opts, args) = parser.parse_args()

	import logging
	import os
	from util import log_setup
	log_setup.setupLogging(opts.log_level)
	log = logging.getLogger('main')

	DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

	account_dir = os.path.join('data', opts.type)
	if not os.path.isdir(account_dir):
		os.mkdir(account_dir)
		# Create starting portfolio
		portfolio = Portfolio(opts.type, datetime.datetime(2010,1,1,0,0,0))
		portfolio.save()

	start_time = datetime.datetime.strptime(opts.start_time, DATETIME_FORMAT)
	end_time = datetime.datetime.strptime(opts.end_time, DATETIME_FORMAT)

	# Run cap gains report
	report = CapGains.report(
		name = opts.type,
		start_time = start_time,
		end_time = end_time)
	printable_report = dict()
	def make_printable(d):
		new_dict = dict()
		for k, v in d.iteritems():
			if isinstance(k, Currency):
				k = CURRENCIES[k.value]
			if isinstance(v, dict):
				new_dict[k] = make_printable(v)
			elif isinstance(v, list):
				new_dict[k] = v
				for i, el in enumerate(v):
					el = make_printable(el)
					new_dict[k][i] = el
			elif isinstance(v, Decimal):
				new_dict[k] = float(v)
			elif isinstance(v, Currency):
				new_dict[k] = CURRENCIES[v.value]
			else:
				new_dict[k] = v
		return new_dict
	printable_report = make_printable(report)
	log.info("CAP GAINS REPORT")
	log.info(json.dumps(printable_report, indent=2, sort_keys=True))

	filename = 'capgains.%s-%s.json' %(
		start_time.strftime(Portfolio.DATETIME_FORMAT),
		end_time.strftime(Portfolio.DATETIME_FORMAT))
	with open(os.path.join(account_dir, filename), 'w') as outfile:
		json.dump(printable_report, outfile, indent=2, sort_keys=True)
