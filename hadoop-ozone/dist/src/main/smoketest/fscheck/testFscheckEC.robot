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
    Run FSCheck With Verbose Block
    Run FSCheck With Verbose Chunk
    Run FSCheck With Verbose Key
    Run FSCheck With Verbose Container

*** Keywords ***
Run FSCheck With Verbose Block
    ${result}=    Execute    ozone sh fscheck --verbosity-level=BLOCK
    Should Contain    ${result}    Key is damaged!
Run FSCheck With Verbose Chunk
    ${result}=    Execute    ozone sh fscheck --verbosity-level=CHUNK
    Should Contain    ${result}    Key is damaged!
Run FSCheck With Verbose Key
    ${result}=    Execute    ozone sh fscheck --verbosity-level=KEY
    Should Contain    ${result}    Key is damaged!
Run FSCheck With Verbose Container
    ${result}=    Execute    ozone sh fscheck --verbosity-level=CONTAINER
    Should Contain    ${result}    Key is damaged!