#!/bin/bash
# Использование:
#  ./version.sh  - только базовый test.py (быстро, без рестартов)
# ./version.sh profile  test_pg_profile.py
# ./version.sh cron  test_pg_cron.py
#./version.sh kcache  test_pg_stat_kcache.py
#./version.sh system_stats  test_system_stats.py
# ./version.sh profile cron kcache system_stats  - всё сразу 

SUITES=("$@")

want() {
    [[ " ${SUITES[*]} " == *" $1 "* ]]
}

declare -A services
services["postgresql15st-server"]="postgresql15"
services["postgresql16-server"]="postgresql16"
services["postgresql17-server"]="postgresql17"
services["postgresql18-server"]="postgresql18"

declare -A pkgs_profile
pkgs_profile["postgresql15st-server"]="postgresql15st-pg_profile postgresql15st-contrib postgresql15st-devel"
pkgs_profile["postgresql16-server"]="postgresql16-pg_profile postgresql16-contrib postgresql16-devel"
pkgs_profile["postgresql17-server"]="postgresql17-pg_profile postgresql17-contrib postgresql17-devel"
pkgs_profile["postgresql18-server"]="postgresql18-pg_profile postgresql18-contrib postgresql18-devel"

declare -A pkgs_cron
pkgs_cron["postgresql15st-server"]="postgresql15st-pg_cron"
pkgs_cron["postgresql16-server"]="postgresql16-pg_cron"
pkgs_cron["postgresql17-server"]="postgresql17-pg_cron"
pkgs_cron["postgresql18-server"]="postgresql18-pg_cron"

declare -A pkgs_kcache
pkgs_kcache["postgresql15st-server"]="postgresql15st-pg_stat_kcache postgresql15st-contrib"
pkgs_kcache["postgresql16-server"]="postgresql16-pg_stat_kcache postgresql16-contrib"
pkgs_kcache["postgresql17-server"]="postgresql17-pg_stat_kcache postgresql17-contrib"
pkgs_kcache["postgresql18-server"]="postgresql18-pg_stat_kcache postgresql18-contrib"

declare -A pkgs_system_stats
pkgs_system_stats["postgresql15st-server"]="postgresql15st-system_stats"
pkgs_system_stats["postgresql16-server"]="postgresql16-system_stats"
pkgs_system_stats["postgresql17-server"]="postgresql17-system_stats"
pkgs_system_stats["postgresql18-server"]="postgresql18-system_stats"

for version in "${!services[@]}"; do
    svc=${services[$version]}
    data_dir="/var/lib/${svc}/data"

    extra=""
    want profile && extra="$extra ${pkgs_profile[$version]}"
    want cron && extra="$extra ${pkgs_cron[$version]}"
    want kcache && extra="$extra ${pkgs_kcache[$version]}"
    want system_stats && extra="$extra ${pkgs_system_stats[$version]}"

    sudo dnf install $version -y
    if [ $? -ne 0 ]; then
        echo "Ошибка установки $version!"
        continue
    fi

    if [ -n "$extra" ]; then
        sudo dnf install $extra -y
        if [ $? -ne 0 ]; then
            echo "Не удалось установить аддоны для $version - соответствующие тесты будут skip/fail"
        fi
    fi

    if [ -d "$data_dir" ]; then
        sudo rm -rf "${data_dir:?}"/*
    fi

    sudo systemctl start $svc
    if [ $? -ne 0 ]; then
        echo "Не удалось запустить $svc"
        sudo journalctl -u $svc --no-pager -n 30
        sudo dnf erase $version $extra -y
        continue
    fi

    if want cron || want kcache || want system_stats; then
        sudo systemctl stop $svc

        libs=""
        want kcache && libs="pg_stat_statements,pg_stat_kcache"
        want cron && libs="${libs:+$libs,}pg_cron"
        want system_stats && libs="${libs:+$libs,}system_stats"

        sudo sed -i "/^shared_preload_libraries/d; /^#shared_preload_libraries/d" "$data_dir/postgresql.conf"
        echo "shared_preload_libraries = '$libs'" | sudo tee -a "$data_dir/postgresql.conf" > /dev/null

        sudo systemctl start $svc
        if [ $? -ne 0 ]; then
            echo "Не удалось запустить $svc после правки postgresql.conf"
            sudo journalctl -u $svc --no-pager -n 30
            sudo dnf erase $version $extra -y
            continue
        fi
    fi

    python3 -m pytest test.py -vv
    want profile && python3 -m pytest test_pg_profile.py -vv
    want cron && python3 -m pytest test_pg_cron.py -vv
    want kcache && python3 -m pytest test_pg_stat_kcache.py -vv
    want system_stats && python3 -m pytest test_system_stats.py -vv

    sudo systemctl stop $svc
    sudo dnf erase $version $extra -y
done