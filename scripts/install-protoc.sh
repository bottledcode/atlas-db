#!/bin/sh

#
# This file is part of Atlas-DB.
#
# Atlas-DB is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of
# the License, or (at your option) any later version.
#
# Atlas-DB is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Atlas-DB. If not, see <https://www.gnu.org/licenses/>.
#
#

#install protoc
#https://github.com/protocolbuffers/protobuf/releases/

OS=$(uname)
OS=$(echo "$OS" | tr '[:upper]' '[:lower]')

if [ "$OS" = "darwin" ]; then
    platform="osx-universal_binary"
else
    platform="linux-x86_64"
fi

TAG="29.3"
wget https://github.com/protocolbuffers/protobuf/releases/download/v${TAG}/protoc-${TAG}-${platform}.zip -O protoc.zip

# shellcheck disable=SC2181
if [ $? -eq 0 ]; then
    unzip protoc.zip -d tools/
    rm -f protoc.zip
else
    echo "wget protoc failed."
    exit 1
fi
