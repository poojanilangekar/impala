# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import struct

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from parquet.ttypes import FileMetaData, ColumnIndex, OffsetIndex, Type
from thrift.protocol import TCompactProtocol
from thrift.transport import TTransport

def get_column_index(filename, file_pos, length):
  with open(filename) as f:

    #Seek to the column_index
    f.seek(file_pos)

    # Read serialized column_index
    serialized_column_index =  f.read(length)

    #Deserialize column_index
    transport = TTransport.TMemoryBuffer(serialized_column_index)
    protocol = TCompactProtocol.TCompactProtocol(transport)
    column_index = ColumnIndex()
    column_index.read(protocol)
    return column_index

def get_offset_index(filename, file_pos, length):
  with open(filename) as f:

    #Seek to the offset_index
    f.seek(file_pos)

    # Read serialized offset_index
    serialized_offset_index =  f.read(length)

    #Deserialize offset_index
    transport = TTransport.TMemoryBuffer(serialized_offset_index)
    protocol = TCompactProtocol.TCompactProtocol(transport)
    offset_index = OffsetIndex()
    offset_index.read(protocol)
    return offset_index