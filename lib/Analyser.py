import os

from models.DataEntry import DataEntry


class Analyser:
    def __init__(self, file):
        self.file = file
        self.data = list()
        self.registry = dict()
        self.write_average_latency = float()
        self.write_min_latency = float()
        self.write_max_latency = float()
        self.read_average_latency = float()
        self.read_min_latency = float()
        self.read_max_latency = float()
        self.timestamp = int()
        self.database = str()
        self.write_consistency = str()
        self.read_consistency = str()

    def prepare_file(self):
        with open(self.file) as file:
            filename_array = os.path.basename(file.name).split("_")
            self.timestamp = int(filename_array[0])
            self.database = filename_array[1]
            self.write_consistency = filename_array[2][1:]
            self.read_consistency = filename_array[2][1:]

            for line in file.readlines():
                if line[0] != '[':
                    raw_args = [x.strip() for x in line.split(',')]
                    worker_type = DataEntry.WRITER if raw_args[0].split(':')[0] == "writer_id" else DataEntry.READER
                    worker_id = raw_args[0].split(':')[1]
                    args = [x.split(':')[1] for x in raw_args[1:len(raw_args):1]]
                    entry = DataEntry(worker_type, worker_id, args[0], args[1], args[2])
                    self.data.append(entry)
                else:
                    raw_args = [x.strip() for x in line.split(',')]
                    if raw_args[0] == "[READ]":
                        if raw_args[1] == "AverageLatency(us)":
                            self.read_average_latency = float(raw_args[2])
                        elif raw_args[1] == "MinLatency(us)":
                            self.read_min_latency = float(raw_args[2])
                        elif raw_args[1] == "MaxLatency(us)":
                            self.read_max_latency = float(raw_args[2])
                    elif raw_args[0] == "[INSERT]":
                        if raw_args[1] == "AverageLatency(us)":
                            self.write_average_latency = float(raw_args[2])
                        elif raw_args[1] == "MinLatency(us)":
                            self.write_min_latency = float(raw_args[2])
                        elif raw_args[1] == "MaxLatency(us)":
                            self.write_max_latency = float(raw_args[2])

    def run(self):
        self.prepare_file()

        inconsistencies_reads_counter = 0
        total_reads = 0
        for entry in self.data:
            if entry.worker_type == DataEntry.WRITER:
                self.registry[entry.key] = (entry.timestamp, entry.version)
            else:
                total_reads = total_reads + 1
                timestamp, version = self.registry.get(entry.key, (0, 0))
                if int(entry.version) < int(version) and int(timestamp) < int(entry.timestamp):
                    inconsistencies_reads_counter = inconsistencies_reads_counter + 1
                    print("key:" + entry.key + ", " +
                          "timestamp_write:" + timestamp + "," +
                          "timestamp_read:" + entry.timestamp + ","+
                          "version_write:" + version + "," +
                          "version_read:" + entry.version)


        print("\nPERFORMANCE MEASUREMENTS")

        print("(1)Average time to perform a read operation = " + str(self.read_average_latency))
        #print("Read Min Latency = " + str(self.read_min_latency))
        #print("Read Max Latency = " + str(self.read_max_latency))

        print("(2)Average time to perform a write operation = " + str(self.write_average_latency))
        #print("Write Min Latency = " + str(self.write_min_latency))
        #print("Write Max Latency = " + str(self.write_max_latency))

        print("\nCONSISTENCY MEASUREMENTS")

        print("Inconsistent reads = "
              + str(inconsistencies_reads_counter))

        consistent_reads_counter = total_reads - inconsistencies_reads_counter
        print("Consistent reads = "
              + str(consistent_reads_counter))
        print("Total reads = "
              + str(total_reads))

        try:
            ratio_consistent_read_total_reads = consistent_reads_counter / total_reads
        except ZeroDivisionError:
            ratio_consistent_read_total_reads = 0
        print("(4)Ratio consistent reads / total reads = "
              + str(ratio_consistent_read_total_reads))
        try:
            ratio_average_r_latency_total_consistency_r_prob = self.read_average_latency/(consistent_reads_counter/total_reads)
        except ZeroDivisionError:
            ratio_average_r_latency_total_consistency_r_prob = 0
        print("(5)Ratio average read latency / total inconsistency read probability = "
              + str(ratio_average_r_latency_total_consistency_r_prob))
        try:
            ratio_average_w_latency_total_inconsistency_r_prob = self.write_average_latency / (consistent_reads_counter / total_reads)
        except ZeroDivisionError:
            ratio_average_w_latency_total_inconsistency_r_prob = 0
        print("(6)Ratio average write latency / total consistency read probability = "
              + str(ratio_average_w_latency_total_inconsistency_r_prob))

        if consistent_reads_counter == 0:
            consistency_score = 0
        else:
            consistency_score = consistent_reads_counter / total_reads * 100

        print("Consistency score = " + str(consistency_score))
