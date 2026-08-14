#!/bin/bash
declare -A services
services["postgresql15st-server"]="postgresql15"
services["postgresql16-server"]="postgresql16"
services["postgresql17-server"]="postgresql17"
services["postgresql18-server"]="postgresql18"

declare -A pg_profile_pkgs
pg_profile_pkgs["postgresql15st-server"]="postgresql15st-pg_profile postgresql15st-contrib"
pg_profile_pkgs["postgresql16-server"]="postgresql16-pg_profile postgresql16-contrib"
pg_profile_pkgs["postgresql17-server"]="postgresql17-pg_profile postgresql17-contrib"
pg_profile_pkgs["postgresql18-server"]="postgresql18-pg_profile postgresql18-contrib"

for version in "${!services[@]}"; do
    sudo dnf install $version -y
    if [ $? -ne 0 ]; then
        echo "Ошибка установки $version!"
        continue
    fi

    sudo dnf install ${pg_profile_pkgs[$version]} -y
    if [ $? -ne 0 ]; then
        echo "Не удалось установить pg_profile/contrib для $version — тест pg_profile будет skip/fail"
    fi

    sudo systemctl start ${services[$version]}
    if [ $? -ne 0 ]; then
        echo "Не удалось запустить ${services[$version]}"
        sudo journalctl -u ${services[$version]} --no-pager -n 30
        sudo dnf erase $version ${pg_profile_pkgs[$version]} -y
        continue
    fi

    python3 -m pytest test.py -v -s
    sudo systemctl stop ${services[$version]}

    sudo dnf erase $version ${pg_profile_pkgs[$version]} -y
done