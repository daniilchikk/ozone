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

*** Variables ***
${TEST_VOLUME}            test-volume
${TEST_BUCKET}            test-bucket

*** Test Cases ***
Verify FSCheck With EC
    [Setup]    Setup Ozone EC Test Environment
    Corrupt Chunk
    Run FSCheck With Verbose Block
    Run FSCheck With Verbose Chunk
    Run FSCheck With Verbose Key
    Run FSCheck With Verbose Container
    Run FSCheck With Delete Option

*** Keywords ***
Setup Ozone EC Test Environment
    Log    Setting up Ozone EC test environment
    Execute And Ignore Error    ozone sh volume create /vol1
    Execute And Ignore Error    ozone sh bucket create --type=EC --replication=rs-3-2-1024k /vol1/bucket1
    Execute    dd if=/dev/urandom of=testfile bs=1M count=10
    Execute    ozone sh key put --type=EC --replication=rs-3-2-1024k /vol1/bucket1/key1 testfile

Corrupt Chunk
    Log    Corrupting Chunk
    ${block_id}=    Execute    ozone sh key info /vol1/bucket1/key1 | jq -r '.ozoneKeyLocations[].localID'
    ${block_path}=    Execute    bash -c "find /data/hdds/hdds -name '*${block_id}.block' | head -n 1"
    Execute    bash -c "dd if=/dev/urandom of=${block_path} bs=1 count=256 seek=256 conv=notrunc"

Run FSCheck With Verbose Key
    Log    Running fscheck with verbose option for keys
    ${result}=    Execute    ozone sh fscheck --keys --verbosity-level=KEY
    Should Contain    ${result}    Key Information:
    Should Contain    ${result}    vol1/bucket1/key1

Run FSCheck With Verbose Chunk
    Log    Running fscheck with verbose option for chunks
    ${result}=    Execute    ozone sh fscheck --keys --verbosity-level=CHUNK
    Should Contain    ${result}    vol1/bucket1/key1
    Should Contain    ${result}    Chunk:

Run FSCheck With Verbose Block
    Log    Running fscheck with verbose option for blocks
    ${result}=    Execute    ozone sh fscheck --keys --verbosity-level=BLOCK
    Should Contain    ${result}    Block commit sequence id:

Run FSCheck With Verbose Container
    Log    Running fscheck with verbose option for containers
    ${result}=    Execute    ozone sh fscheck --keys --verbosity-level=CONTAINER
    Should Contain    ${result}    Container

Run FSCheck With Delete Option
    Log    Running fscheck with delete option
    ${check}=    Execute    ozone sh fscheck --delete --verbosity-level=KEY
    Should Contain    ${check}    Key is damaged!
    ${result}=    Execute    ozone sh key list /vol1/bucket1
    Should Contain    ${result}    [ ]