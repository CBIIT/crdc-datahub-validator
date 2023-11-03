#!/usr/bin/env python3
import pandas as pd
from bento.common.utils import get_logger
# This script load matadata files to database
# input: file info list
class DataLoader:
    def __init__(self, configs):
        self.log = get_logger('Matedata loader')

    def load_data(self, data_frame_list):
        return True