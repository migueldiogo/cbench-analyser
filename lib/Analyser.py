from __future__ import division

import os
from models.DataEntry import DataEntry


class Analyser:
    def __init__(self, input_filename, output_filename, print_header=True):
        self.input_filename = input_filename
        self.output_filename = output_filename
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
        self.ratio_read_latency_and_consistency_score = float()
        self.ratio_write_latency_and_consistency_score = float()
        self.consistency_score = int()
        self.total_operations = 0
        self.unavailable_service_counter = 0
        self.availability_score = int()
        self.print_header = print_header

    def prepare_file(self):
        with open(self.input_filename) as file:
            filename_array = os.path.basename(file.name).split("_")
            self.timestamp = int(filename_array[0])
            self.database = filename_array[1]
            self.write_consistency = filename_array[2][1:]
            self.read_consistency = filename_array[2][1:]

            for line in file.readlines():
                if line[0] != '[':
                    self.total_operations = self.total_operations + 1
                    raw_args = [x.strip() for x in line.split(',')]
                    worker_type = DataEntry.WRITER if raw_args[0].split(':')[0] == "writer_id" else DataEntry.READER
                    worker_id = raw_args[0].split(':')[1]
                    args = [x.split(':')[1] for x in raw_args[1:len(raw_args):1]]

                    if 'UNAVAILABLE' in args[2]:
                        self.unavailable_service_counter = self.unavailable_service_counter + 1
                    else:
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

            self.data = sorted(self.data, key=lambda item: item.timestamp)

    def write_header(self, file):
        file.write("database, "
                   "write_consistency, "
                   "read_consistency, "
                   "average_time_read, "
                   "average_time_write, "
                   "consistency_score, "
                   "ratio_read_latency_and_consistency_score, "
                   "ratio_write_latency_and_consistency_score, "
                   "total_operations, "
                   "availability_score\n")

    def write_data(self, file):
        file.write(
            str(self.database) + "," +
            str(self.write_consistency) + "," +
            str(self.read_consistency) + "," +
            str(self.read_average_latency) + "," +
            str(self.write_average_latency) + "," +
            str(self.consistency_score) + "," +
            str(self.ratio_read_latency_and_consistency_score) + "," +
            str(self.ratio_write_latency_and_consistency_score) + "," +
            str(self.total_operations) + "," +
            str(self.availability_score) + "\n"

        )

    def calculate(self):
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
                    print(entry)

        consistent_reads_counter = total_reads - inconsistencies_reads_counter

        try:
            self.ratio_read_latency_and_consistency_score = self.read_average_latency / (
                    consistent_reads_counter / total_reads)
        except ZeroDivisionError:
            self.ratio_read_latency_and_consistency_score = 0
        try:
            self.ratio_write_latency_and_consistency_score = self.write_average_latency / (
                    consistent_reads_counter / total_reads)
        except ZeroDivisionError:
            self.ratio_write_latency_and_consistency_score = 0

        if consistent_reads_counter == 0:
            self.consistency_score = 0
        else:
            self.consistency_score = consistent_reads_counter / total_reads * 100

        if self.unavailable_service_counter == self.total_operations:
            self.availability_score = 0
        else:
            self.availability_score = (self.total_operations - self.unavailable_service_counter) / self.total_operations * 100

    def run(self):
        self.prepare_file()
        self.calculate()

        with open(self.output_filename, "a+") as file:
            if self.print_header:
                self.write_header(file)

            self.write_data(file)
