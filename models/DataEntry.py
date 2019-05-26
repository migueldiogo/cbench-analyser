class DataEntry:
    WRITER = 1
    READER = 2

    def __init__(self, worker_type, worker_id, key, timestamp, version):
        self.worker_type = worker_type
        self.worker_id = worker_id
        self.key = key
        self.timestamp = timestamp
        self.version = version
