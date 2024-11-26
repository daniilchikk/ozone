# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

*** Settings ***
Documentation       Smoketest ozone cluster startup with corrupted key test
Library             OperatingSystem
Library             String
Library             Collections
Library             Process
Resource            ../commonlib.robot
Resource            ../ozone-lib/shell.robot

*** Test Cases ***
Corrupt Key
    Corrupt Chunks

*** Keywords ***
Corrupt Chunks
    ${block_id}=    Execute    ozone sh key info /vol1/bucket1/key1 | jq -r '.ozoneKeyLocations[].localID'
    ${block_path}=    Execute    bash -c "find /data/hdds/hdds -name '*${block_id}.block' | head -n 1"
    Execute    bash -c "dd if=/dev/urandom of=${block_path} bs=1 count=256 seek=256 conv=notrunc"


