import os

datadir = os.environ['OPENSHIFT_DATA_DIR']  # /var/data/
internal_ip = os.environ['OPENSHIFT_INTERNAL_IP']  # 127.0.0.1
tmpdir = os.environ['OPENSHIFT_TMP_DIR']

try:
	import local_settings
except ImportError:
	pass
