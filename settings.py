datadir = "/var/storage/"  # os.environ['OPENSHIFT_DATA_DIR']
internal_ip = "127.0.0.1"  # os.environ['OPENSHIFT_INTERNAL_IP']
tmpdir = "/tmp/"  # os.environ['OPENSHIFT_TMP_DIR']
staticdir = "/var/www/"  # os.path.join(os.environ['OPENSHIFT_REPO_DIR'], "app/static")

chunk_size = 2 * 1024 * 1024  # 2 MB

chunks_watch_limit = 100
chunks_threshold = 30

import sh
import glob


def used_space():
    return int(sh.sed(sh.awk(sh.quota(), '{print $1}'), '-n', '4p')) * 1024


def full_space():
    return int(sh.sed(sh.awk(sh.quota(), '{print $3}'), '-n', '4p')) * 1024


def free_space():
    return full_space() - used_space()


def available_chunks():
    return free_space() / chunk_size

FILENAMES = []

try:
    from local_settings import *
except ImportError:
    pass

FILENAMES += glob.glob(datadir + "*.log")
