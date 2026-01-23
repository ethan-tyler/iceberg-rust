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


install:
	uv sync --group dev --no-install-project

build:
	uv run maturin develop

test:
	uv run --no-sync pytest

# Download and install a DataFusion python wheel artifact (e.g. from a PR CI run).
# Defaults target DF52 upgrade PR artifact.
DF_RUN_ID ?= 21139947370
DF_REPO ?= apache/datafusion-python
DF_ARTIFACT ?= dist-macos-latest
DF_WHEEL_DIR ?= wheels/df52

download-datafusion-wheel:
	rm -rf "$(DF_WHEEL_DIR)"
	mkdir -p "$(DF_WHEEL_DIR)"
	gh run download "$(DF_RUN_ID)" -R "$(DF_REPO)" -n "$(DF_ARTIFACT)" --dir "$(DF_WHEEL_DIR)"

install-datafusion-wheel: download-datafusion-wheel
	uv run --no-sync python -m ensurepip --upgrade
	uv run --no-sync python -m pip install --force-reinstall "$(DF_WHEEL_DIR)"/*.whl
