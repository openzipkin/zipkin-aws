#!/usr/bin/env bash
#
# Copyright 2016-2017 The OpenZipkin Authors
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
# in compliance with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under
# the License.
#

set -euo pipefail
set -x

if printf 'VERSION=${project.version}\n0\n' | ./mvnw org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate | grep '^VERSION'  | grep -q SNAPSHOT; then
    ./mvnw --batch-mode -s ./.settings.xml -Prelease -nsu -DskipTests deploy
else
    echo "Not building release versions, those are built by the tag builder"
fi
