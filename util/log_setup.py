import logging

class CustomFormatter(logging.Formatter):
	def __init__(self,
		format,
		add_manager_time,
		add_pid):
		super(CustomFormatter, self).__init__(format)
		self._add_manager_time = add_manager_time
		self._add_pid = add_pid

	def format(self, record):
		if not (self._add_manager_time and hasattr(record, 'manager_time')):
			record.manager_time = ''
		if not (self._add_pid and hasattr(record, 'pid')):
			record.pid = ''
		record.shortlevel = record.levelname[:3]
		return super(CustomFormatter, self).format(record)

def setupLogging(
	log_level,
	add_manager_time = True,
	add_pid = True):
	level = logging._levelNames.get(log_level.upper())
	logStreamHandler = logging.StreamHandler()
	logStreamHandler.setLevel(level)
	format = '%(asctime)s %(manager_time)s%(pid)5s%(shortlevel)s %(filename)s:%(lineno)d %(name)s:%(funcName)s %(message)s'
	formatter = CustomFormatter(
		format = format,
		add_manager_time = add_manager_time,
		add_pid = add_pid)
	logStreamHandler.setFormatter(formatter)
	logging.getLogger().addHandler(logStreamHandler)
	logging.getLogger().setLevel(level)

def setLogLevel(log_level):
	level = logging._levelNames.get(log_level.upper())
	logging.getLogger().setLevel(level)
