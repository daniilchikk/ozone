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
Documentation       Smoketest ozone create container sequence ID generator
Library             OperatingSystem
Library             BuiltIn
Resource            ../commonlib.robot
Test Timeout        5 minutes

*** Variables ***

** Keywords ***
Create container
    ${output} =                 Execute          ozone admin container create
                                Should contain   ${output}   is created

    ${container} =              Execute          echo "${output}" | grep 'is created' | cut -f2 -d' '
    [return]                    ${container}

Start SCM
                                Open Connection And Log In
    ${rc1} =                    Execute Command                 /opt/startSCM.sh                return_stdout=False    return_rc=True
                                Should Be Equal As Integers     ${rc1}                  0
    ${startMsg}  ${rc2} =       Execute Command                 sudo ps aux | grep scm          return_rc=True
                                Should Be Equal As Integers     ${rc2}                  0
                                Close Connection
                                Should Contain                  ${startMsg}             OzoneManagerStarter

Stop SCM
                                Open Connection And Log In
    ${rc1} =                    Execute Command                 /opt/stopSCM.sh                 return_stdout=False    return_rc=True
                                Should Be Equal As Integers     ${rc1}                  0
    ${stopMsg}  ${rc2} =        Execute Command                 sudo ps aux | grep scm          return_rc=True
                                Should Be Equal As Integers     ${rc2}                  0
                                Close Connection
                                Should Not Contain              ${stopMsg}              OzoneManagerStarter

*** Test Cases ***
Create containers sequentially
    # Create new container in SCM
    ${firstContainer} =         Create container

    # Create second container in SCM and check if ID is different
    ${secondContainer} =        Create container
    ${expectedContainer} =      Evaluate                        ${firstContainer} + 1
                                Should be Equal as Integers     ${expectedContainer}     ${secondContainer}

Create containers sequentially and transfer SCM leader
    # Create new container in SCM
    ${firstContainer} =         Create container

    # Transfer leadership to the Follower SCM
    ${result} =                 Execute                 ozone admin scm transfer --service-id=scmservice -r
                                LOG                     ${result}
                                Should Contain          ${result}               Transfer leadership successfully

    # Create second container in SCM and check if ID is different
    ${secondContainer} =        Create container
    ${unexpectedContainer} =    Evaluate                ${firstContainer} + 1
                                Should not be Equal     ${unexpectedContainer}  ${secondContainer}
                                Should be Equal         1001                    ${secondContainer}

Create containers sequentially and restart SCM
    # Create new container in SCM
    ${firstContainer} =         Create container

    # Transfer leadership to the Follower SCM
    ${result} =                 Execute                 ozone admin scm transfer --service-id=scmservice -r
                                LOG                     ${result}
                                Should Contain          ${result}               Transfer leadership successfully

    # Create second container in SCM and check if ID is different
    ${secondContainer} =        Create container
    ${unexpectedContainer} =    Evaluate                ${firstContainer} + 1
                                Should not be Equal     ${unexpectedContainer}     ${secondContainer}
                                Should be Equal         2001     ${secondContainer}
