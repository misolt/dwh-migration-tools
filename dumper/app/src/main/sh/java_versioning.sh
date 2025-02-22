#!/bin/bash
# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

function is_java_greater_than_8() {
  java_version_output="$(java -version 2>&1)"

  # Will return 1 for all versions before Java 9.
  java_major_version="$(echo "$java_version_output" | grep -Eoi 'version "?([0-9]+)' | head -1 | cut -d'"' -f2 | cut -d'.' -f1)"

  # Check if the major version is greater than 8.
  if [[ "$java_major_version" -gt 8 ]]; then
    return 0  # True
  else
    return 1  # False
  fi
}
