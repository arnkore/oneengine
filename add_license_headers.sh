#!/bin/bash

# Apache 2.0 License header
LICENSE_HEADER="/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the \"License\"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an \"AS IS\" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

"

# Function to add license header to a file
add_license_to_file() {
    local file="$1"
    
    # Check if file already has license header
    if head -n 20 "$file" | grep -q "Licensed to the Apache Software Foundation"; then
        echo "Skipping $file - already has license header"
        return
    fi
    
    # Create temporary file with license header
    local temp_file=$(mktemp)
    echo "$LICENSE_HEADER" > "$temp_file"
    
    # Add original file content
    cat "$file" >> "$temp_file"
    
    # Replace original file
    mv "$temp_file" "$file"
    
    echo "Added license header to $file"
}

# Find all Rust source files and add license headers
echo "Adding Apache 2.0 license headers to Rust source files..."

# Process files in src directory
find src -name "*.rs" | while read -r file; do
    add_license_to_file "$file"
done

# Process files in examples directory
find examples -name "*.rs" | while read -r file; do
    add_license_to_file "$file"
done

# Process files in tests directory
find tests -name "*.rs" | while read -r file; do
    add_license_to_file "$file"
done

# Process files in benches directory
find benches -name "*.rs" | while read -r file; do
    add_license_to_file "$file"
done

echo "License header addition completed!"
