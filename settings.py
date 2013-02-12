datadir = "/var/storage/"  # os.environ['OPENSHIFT_DATA_DIR']
internal_ip = "127.0.0.1"  # os.environ['OPENSHIFT_INTERNAL_IP']
tmpdir = "/tmp/"  # os.environ['OPENSHIFT_TMP_DIR']

try:
    from local_settings import *
except ImportError:
    pass
