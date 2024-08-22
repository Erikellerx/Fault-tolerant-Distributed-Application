
class Message:

	def __init__(self, name, data = 'None', server_name = None, host = '', port = 5000, request_num = 0, checkpoint = None, state = None, ready = None):
		self.name = name
		self.data = data
		self.server_name = server_name
		self.host = host
		self.port = port
		self.request_num = request_num
		self.checkpoint = checkpoint
		self.state = state
		self.ready = ready

