#!/bin/bash
trunk_install_failed_extensions=()
need_load_shared_preload_libraries_extensions=()
version_not_found_extensions=()
file='/tmp/extensions.txt'
extension_count=$(<$file wc -l)
lines=$(cat $file)
for line in $lines
do
        output=$(trunk install $line 2>&1)

        if [ $? -ne 0 ]; then
            if [[ $output == *"Failed to find an archive for"* || $output == *"Failed to fetch Trunk archive from"* ]]; then
                version_not_found_extensions+=("$line")
            else
                echo "trunk install command failed"
                trunk_install_failed_extensions+=("$line")
            fi
        fi
        echo $output
        printf "\n\n"
done
IFS=$'\n' extensions=(`psql postgres://postgres:postgres@localhost:5432 -tA postgres -c 'select name from pg_available_extensions;'`)
for ext in "${extensions[@]}"
do
        # drop schema columnar if ext name is columnar
        if [ "$ext" == "columnar" ]; then
            psql postgres://postgres:postgres@localhost:5432 -c "drop extension if exists citus_columnar cascade;"
        fi
        # drop type semver if ext name is semver
        if [ "$ext" == "semver" ]; then
            psql postgres://postgres:postgres@localhost:5432 -c "drop extension if exists pg_text_semver cascade;"
        fi
        # if extension name is meta_triggers, create extension meta first and create extension meta_triggers
        if [ "$ext" == "meta_triggers" ]; then
            psql postgres://postgres:postgres@localhost:5432 -c "create extension if not exists hstore cascade;"
            psql postgres://postgres:postgres@localhost:5432 -c "create extension if not exists meta cascade;"
            psql postgres://postgres:postgres@localhost:5432 -c "create extension if not exists meta_triggers cascade;"
        fi
        output=$(psql postgres://postgres:postgres@localhost:5432 -c "create extension if not exists \"$ext\" cascade;" 2>&1)
        if [ $? -ne 0 ]; then
            if [[ $output == *"shared_preload_libraries"* ]]; then
                need_load_shared_preload_libraries_extensions+=("$ext")
            elif [[ $output == *"already exists"* ]]; then
                echo "extension \"$ext\" already exists"
            else
                echo "CREATE EXTENSION command failed"
                failed_extensions+=("$ext")
            fi
        fi
        echo $output
        printf "\n\n"
done
available_extensions_count=${#extensions[@]}
failure_count=${#failed_extensions[@]}
need_load_shared_preload_libraries_count=${#need_load_shared_preload_libraries_extensions[@]}
not_found_count=${#version_not_found_extensions[@]}
success=$(($available_extensions_count-$failure_count))
success_percent=$(awk "BEGIN { pc=100*${success}/${extension_count}; i=int(pc); print (pc-i<0.5)?i:i+1 }")
failure_percent=$(awk "BEGIN { pc=100*${failure_count}/${extension_count}; i=int(pc); print (pc-i<0.5)?i:i+1 }")

printf "\n\n***TRUNK INSTALL EXTENSIONS THAT VERSION NOT FOUND RATE***\n"
echo "$not_found_count / $extension_count"
printf "***CREATE EXTENSIONS SUCCESS RATE***\n"
echo "$success / $extension_count ($success_percent%)"
printf "\n\n***CREATE EXTENSION FAILURE RATE***\n"
echo "$failure_count / $extension_count ($failure_percent%)"
printf "\n\n***CREATE EXTENSIONS THAT NEED TO BE LOADED IN shared_preload_libraries***\n"
echo "$need_load_shared_preload_libraries_count / $extension_count" 
printf "\n\n***NEED TO LOAD shared_preload_libraries EXTENSIONS***\n"
for need in "${need_load_shared_preload_libraries_extensions[@]}"
do
      echo $need
done

printf "\n\n***FAILED EXTENSIONS***\n"
for failed in "${failed_extensions[@]}"
do
      echo $failed
done
