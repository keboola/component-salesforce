import os
import json
import logging
from keboola.component.dao import TableDefinition


class InterimBuffer:
    current_id = 0

    def __init__(self, manager, chunk):
        self.manager = manager
        self.row_count = len(chunk)
        self.error = len(chunk)
        self.success = 0
        self.result_file_path = None
        self.finished_job = None
        self.result = None
        self.job_id = None
        self.result_path = None
        self.job_error_message = None
        self.result_table = manager.result_table
        self.data_folder_path = manager.data_folder_path
        self.serial_mode = manager.serial_mode
        self.processed = False
        self.id = InterimBuffer._get_id()
        self.file_name = f'{self.id}'
        self.file_path = self._get_temp_file_path()
        self.save(chunk)

    @staticmethod
    def _get_id():
        InterimBuffer.current_id += 1
        return InterimBuffer.current_id

    def _get_temp_folder(self):
        tmp_folder = os.path.join(self.data_folder_path, 'tmp')
        os.makedirs(tmp_folder, exist_ok=True)
        return tmp_folder

    def _get_temp_file_path(self):
        return os.path.join(self._get_temp_folder(), self.file_name)

    def add_job(self, job_id):
        self.job_id = job_id
        new_file_name = f'{self.id}_{self.job_id}'
        logging.debug(f'Added job id to a buffer and rename buffer file to {new_file_name}')
        new_file_path = os.path.join(os.path.dirname(self.file_path), new_file_name)
        os.rename(self.file_path, new_file_path)
        self.file_name = new_file_name
        self.file_path = new_file_path
        return new_file_path

    def save(self, chunk):
        self.error = len(chunk)
        with open(self.file_path, 'a') as buffer_file:
            json.dump(chunk, buffer_file)
            buffer_file.write('\n')  # Add a newline for each chunk
        logging.debug(f'Written chunk "{self.file_name}" to buffer file')
        return self

    def get_buffer_data(self):
        with open(self.file_path, 'r') as buffer_file:
            return json.loads(buffer_file.read())

    def process_done(self):
        self.processed = True
        logging.debug(f"Buffer {self.id} processed. Removing buffer file {self.file_path}")
        if os.path.exists(self.file_path):
            os.remove(self.file_path)

    def finish_job(self, result):
        self.finished_job = True
        self.result = result


class InterimBufferManager:
    def __init__(self, data_folder, result_table: TableDefinition, serial_mode):
        self.buffers = []
        self.result_table = result_table
        self.data_folder_path = data_folder
        self.serial_mode = serial_mode

    def create_buffer(self, chunk):
        buffer = InterimBuffer(self, chunk)
        self.buffers.append(buffer)
        return buffer

    def get_buffers(self):
        return self.buffers

    def finished_jobs(self):
        return sum(1 for buffer in self.buffers if buffer.finished_job)

    def unfinished_jobs(self):
        return [buffer for buffer in self.buffers if not buffer.finished_job]

    def total_success(self):
        return sum(buffer.success for buffer in self.buffers)

    def total_error(self):
        return sum(buffer.error for buffer in self.buffers)

    def total_rows(self):
        return sum(buffer.row_count for buffer in self.buffers)

    def total_unprocessed_buffers(self):
        return sum(1 for buffer in self.buffers if not buffer.processed)

    def unprocessed_buffers(self):
        return [buffer for buffer in self.buffers if not buffer.processed]
