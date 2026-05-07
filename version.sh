#!/bin/bash

# Определение списка версий
#versions=("postgresql15st-server" "postgresql16-server" "postgresql17-server" "postgresql18-server")
declare -A services
services["postgresql15st-server"]="postgresql15"
services["postgresql16-server"]="postgresql16"
services["postgresql17-server"]="postgresql17"
services["postgresql18-server"]="postgresql18"
# Перебор массива
for version in "${!services[@]}"; do
    #echo "Обработка версии: $version"
    
    sudo dnf install $version -y
    if [ $? -ne 0 ]; then
    echo "Ошибка!"
    continue
    fi
    sudo systemctl start ${services[$version]}
    pytest test.py -v -s
    sudo systemctl stop ${services[$version]}
    sudo dnf erase $version -y
done
