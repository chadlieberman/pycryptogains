import json, hmac, hashlib, time, requests, base64, logging, datetime
from requests.auth import AuthBase
import re
from calendar import timegm
from decimal import Decimal

from database import get_db_session

from models import Pagination, Account

UNIX_EPOCH_START = datetime.datetime(1970, 1, 1, 0, 0, 0)
DATE_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

class CoinbaseRestException(Exception):
	def __init__(self, message):
		self._message = message

	def __str__(self):
		return repr(self._message)

class ExchangeAuth(AuthBase):
	def __init__(self,
		api_key,
		secret,
		passphrase):
		self.api_key = api_key
		self.secret = secret
		self.passphrase = passphrase

	def __call__(self, request):
		timestamp = str(time.time())
		message = timestamp + request.method + request.path_url + (request.body or '')
		encoded_message = message.encode('utf8')
		hmac_key = base64.b64decode(self.secret)
		signature = hmac.new(hmac_key, encoded_message, hashlib.sha256)
		signature_b64 = base64.b64encode(signature.digest()).decode()

		request.headers.update({
			'CB-ACCESS-SIGN': signature_b64,
			'CB-ACCESS-TIMESTAMP': timestamp,
			'CB-ACCESS-KEY': self.api_key,
			'CB-ACCESS-PASSPHRASE': self.passphrase,
			'Content-Type': 'application/json'
		})
		return request

def get_account_from_base_url(base_url):
	if base_url.find('prime.coinbase.com') > -1:
		return Account.CoinbasePrime
	elif base_url.find('pro.coinbase.com') > -1:
		return Account.CoinbasePro
	else:
		raise Exception('Could not determine account from base_url "%s"' %(base_url))

def get_product_id_from_uri(uri):
	match = re.match('^.*product_id=([A-Z-]+).*$', uri)
	if match is None:
		return None
	return match.groups()[0]

class AuthRequesterFactory(object):
	def __init__(self, credentials):
		self._auth = ExchangeAuth(
			api_key = credentials['api_key'],
			secret = credentials['secret'],
			passphrase = credentials['passphrase'])
		self._base_url = 'https://' + credentials['base_url']

	def __call__(self, uri):
		method = 'GET'
		url = self._base_url + uri
		time.sleep(0.25)
		response = requests.request(
			method = method,
			url = url,
			auth = self._auth,
			verify = True)
		return response.json()

	def generate(self, uri, paginate_until=None, after=None):
		method = 'GET'
		response_data = []
		url = self._base_url + uri
		response = requests.request(
			method = method,
			url = url,
			params = {'after': after},
			auth = self._auth,
			verify = True)
		print(response)
		print(response.text)
		if isinstance(response.json(), list):
			response_data += response.json()
		else:
			yield response.json()
		if paginate_until is None:
			for r in response_data:
				yield r
		else:
			db_session = get_db_session()
			pagination_key = paginate_until['key']
			pagination_condition = paginate_until['condition']
			if len(response_data) == 0:
				return
			for r in response_data:
				if pagination_condition(r[pagination_key]):
					yield r
			while pagination_condition(response_data[-1][pagination_key]):
				if 'CB-AFTER' in response.headers:
					time.sleep(0.25) # To avoid rate limit
					after = response.headers['CB-AFTER']
					response = requests.request(
						method=method, url=url, auth=self._auth, params={'after': after})
					response_data = response.json()
					if not isinstance(response_data, list):
						response_data = [response_data]
					if len(response_data) == 0:
						break
					for r in response_data:
						if pagination_condition(r[pagination_key]):
							yield r
					try:
						# Record pagination info in DB
						pagination = Pagination(
							account = get_account_from_base_url(self._base_url),
							product_id = get_product_id_from_uri(uri),
							url = url,
							start_time = response_data[-1]['created_at'],
							end_time = response_data[0]['created_at'],
							cursor_before = response.headers['CB-BEFORE'],
							cursor_after = response.headers['CB-AFTER'])
						db_session.add(pagination)
						db_session.commit()
					except Exception as e:
						print(str(e))
						# Let this go since it's just extra info that is nice to have
				else:
					break
			db_session.remove()

class Api(object):
	def __init__(self, api_config):
		self._api_config = api_config
		self._requester = AuthRequesterFactory(api_config)

	def get_usd_price(self, currency, dt):
		print('get_usd_price(): currency=%s, dt=%s' %(currency, dt))
		if currency == 'BCHSV':
			return Decimal(0.0)
		if currency == 'XRP':
			if dt == datetime.datetime(2018,1,1):
				return Decimal(2.300000)
			elif dt == datetime.datetime(2018,2,1):
				return Decimal(1.16)
			elif dt == datetime.datetime(2018,3,1):
				return Decimal(0.904131)
			elif dt == datetime.datetime(2018,4,1):
				return Decimal(0.513854)
			elif dt == datetime.datetime(2018,5,1):
				return Decimal(0.838355)
			elif dt == datetime.datetime(2018,6,1):
				return Decimal(0.612893)
			elif dt == datetime.datetime(2018,7,1):
				return Decimal(0.465944)
			elif dt == datetime.datetime(2018,8,1):
				return Decimal(0.435562)
			elif dt == datetime.datetime(2018,9,1):
				return Decimal(0.335313)
			elif dt == datetime.datetime(2018,10,1):
				return Decimal(0.583511)
			elif dt == datetime.datetime(2018,11,1):
				return Decimal(0.448620)
			elif dt == datetime.datetime(2018,12,1):
				return Decimal(0.362557)
			elif dt == datetime.datetime(2019,1,1):
				return Decimal(0.352512)
			elif dt == datetime.datetime(2019,3,4):
				return Decimal(0.312021)
			else:
				raise Exception('No XRP price for that date')
		if isinstance(dt, str):
			dt = datetime.datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S.%fZ')
		product_id = '%s-USD' %(currency)
		# Get candles by the minute around the requested time
		end = dt + datetime.timedelta(minutes = 150)
		start = dt - datetime.timedelta(minutes = 150)
		uri = '/products/%s/candles?start=%s&end=%s&granularity=60' %(
			product_id,
			start.isoformat(),
			end.isoformat())
		response = self._requester(uri)
		if not isinstance(response, list) or len(response) == 0:
			raise Exception('Unexcepted non-list or empty response')
		# Determine the candle that is closest without being under
		requested_time = timegm(dt.timetuple())
		relevant_candle = next(v for ind, v in enumerate(response) if v[0] <= requested_time)
		candle_time, low, high, open, close, volume = relevant_candle
		if abs(candle_time - requested_time) > 300.0:
			raise Exception('Could not find price within 5 minute of requested time')
		return Decimal("%1.8f" %(close))

	def get_fills(self, product_id, backwards_until, after=None):
		def condition(time):
			return time >= backwards_until
		paginate_until = {
			'key': 'created_at',
			'condition': condition
		}
		endpoint = '/fills?product_id=%s' %(product_id)
		for fill in self._requester.generate(endpoint, paginate_until, after):
			yield fill

from config import api_config
CoinbasePrimeApi = Api(api_config['CoinbasePrime'])
CoinbaseProApi = Api(api_config['CoinbasePro'])

if __name__ == '__main__':

	import datetime
	import json
	#for fill in CoinbasePrimeApi.get_fills('BTC-USD', '2019-02-20T14:12:46.000Z'):
	#	print(json.dumps(fill, indent=2, sort_keys=True))

	price = CoinbaseProApi.get_usd_price('ETH', '2018-12-31T21:59:59.999Z')
	print(price)
