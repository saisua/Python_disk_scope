
from ftplib import FTP_TLS

class Ftp:
	_ftp_: FTP_TLS
	def __init__(self,
			host='',
			user='',
			passwd='',
			acct=''	  
		) -> None:
		self._ftp_ = FTP_TLS(

		)