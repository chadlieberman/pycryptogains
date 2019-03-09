from sqlalchemy import Table, Column, Integer, DateTime, Enum, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import expression
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.types import DateTime, Numeric

import enum

class utcnow(expression.FunctionElement):
    type = DateTime()

@compiles(utcnow, 'postgresql')
def pg_utcnow(element, compiler, **kw):
    return "TIMEZONE('utc', CURRENT_TIMESTAMP)"

Base = declarative_base()

class Type(enum.Enum):
	Unknown = 0
	Trade = 1
	Transfer = 2

class Account(enum.Enum):
	External = 0
	Coinbase = 1
	CoinbasePro = 2
	CoinbasePrime = 3
	Poloniex = 4

class Currency(enum.Enum):
	USD = 0
	BTC = 1
	ETH = 2
	LTC = 3
	EUR = 4
	BCH = 5
	ETC = 6
	GBP = 7
	ZRX = 8
	USDC = 9
	BAT = 10
	BCHSV = 11
	XRP = 12

CURRENCIES = (
	'USD',
	'BTC',
	'ETH',
	'LTC',
	'EUR',
	'BCH',
	'ETC',
	'GBP',
	'ZRX',
	'USDC',
	'BAT',
	'BCHSV',
	'XRP',
)

Amount = Numeric(precision=20, scale=12)

class Transaction(Base):
	__tablename__ = 'transactions'

	id = Column(Integer, primary_key=True, nullable=False)
	type = Column(Enum(Type), default=Type.Trade, nullable=False)
	external_id = Column(String, unique=True, nullable=True) # <trade_id>:<order_id> for Coinbase
	from_account = Column(Enum(Account), nullable=False)
	from_currency = Column(Enum(Currency), nullable=False)
	from_amount = Column(Amount, nullable=False)
	to_account = Column(Enum(Account), nullable=False)
	to_currency = Column(Enum(Currency), nullable=False)
	to_amount = Column(Amount, nullable=False)
	usd_value = Column(Amount, nullable=False)
	fee = Column(Amount, default=0.0, nullable=False)
	transacted_at = Column(DateTime, index=True, nullable=False)
	ingested_at = Column(DateTime, server_default=utcnow())

	def __str__(self):
		return self.__repr__()

	def __repr__(self):
		record = "\n[Transaction\n"
		record += "  id: %s\n" %(self.id or '(unknown)')
		record += "  type: %s\n" %(self.type)
		record += "  external_id: %s\n" %(self.external_id)
		record += "  from_account: %s\n" %(self.from_account)
		record += "  from_currency: %s\n" %(self.from_currency)
		record += "  from_amount: %1.12f\n" %(self.from_amount)
		record += "  to_account: %s\n" %(self.to_account)
		record += "  to_currency: %s\n" %(self.to_currency)
		record += "  to_amount: %1.12f\n" %(self.to_amount)
		record += "  usd_value: %1.12f\n" %(self.usd_value)
		record += "  fee: %1.12f\n" %(self.fee)
		record += "  transacted_at: %s\n" %(self.transacted_at)
		record += "  ingested_at: %s\n" %(self.ingested_at)
		record += "]\n"
		return record

class Pagination(Base):
	__tablename__ = 'paginations'

	id = Column(Integer, primary_key=True, nullable=False)
	account = Column(Enum(Account), nullable=False)
	product_id = Column(String, nullable=False)
	url = Column(String, nullable=False)
	start_time = Column(DateTime, nullable=False)
	end_time = Column(DateTime, nullable=False)
	cursor_before = Column(String, nullable=True)
	cursor_after = Column(String, nullable=True)

	def __str__(self):
		return self.__repr__()

	def __repr__(self):
		record = "\n[Pagination\n"
		record += "  id: %s\n" %(self.id or '(unknown)')
		record += "  account: %s\n" %(self.account)
		record += "  product_id: %s\n" %(self.product_id)
		record += "  url: %s\n" %(self.url)
		record += "  start_time: %s\n" %(self.start_time)
		record += "  end_time: %s\n" %(self.end_time)
		record += "  cursor_before: %s\n" %(self.cursor_before)
		record += "  cursor_after: %s\n" %(self.cursor_after)
		record += "]\n"
		return record
