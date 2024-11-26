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
Setup Ozone EC
    Setup Ozone Test Environment

*** Keywords ***
Setup Ozone Test Environment
    Log    Setting up Ozone EC test environment...
    Execute    ozone sh volume create /vol1
    Execute    ozone sh bucket create --type=EC --replication=rs-3-2-1024k /vol1/bucket1
    Execute    dd if=/dev/zero of=testfile bs=1M count=10
    Execute    ozone sh key put --type=EC --replication=rs-3-2-1024k /vol1/bucket1/key1 testfile
