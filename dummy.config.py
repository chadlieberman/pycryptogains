
# Edit credentials for your PostgresQL database
# and save as config.py
db_config = {
	'username': '<USERNAME>'
	'password': '<PASSWORD>'
	'host': '<HOSTNAME>'
	'dbname': '<DBNAME>'
}

# Edit credentials for the various apis you need
# to ingest transactions from
api_config = {
	'CoinbasePrime': {
		'base_url': 'api.prime.coinbase.com',
		'api_key': '<API_KEY>',
		'secret': '<SECRET>',
		'passphrase': '<PASSPHRASE>'
	},
	# ...Any other apis
}
