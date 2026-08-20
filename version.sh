#!/bin/bash
# Использование:
#  ./version.sh  - только базовый test.py (быстро, без рестартов)
# ./version.sh profile  test_pg_profile.py
# ./version.sh cron  test_pg_cron.py
#./version.sh kcache  test_pg_stat_kcache.py
#./version.sh system_stats  test_system_stats.py
#./version.sh wait_sampling  test_pg_wait_sampling.py
#./version.sh test_pgaudit.py
#./version.sh test_pgauditlogtofile.py
# ./version.sh all  - все тесты

SUITES=("$@")

want() {
    if [[ " ${SUITES[*]} " == *" all "* ]]; then
        return 0
    fi
    [[ " ${SUITES[*]} " == *" $1 "* ]]
}
wait_port_free() {
    local waited=0
    local timeout=15
    while sudo ss -ltn 2>/dev/null | grep -q ':5432 '; do
        sleep 1
        waited=$((waited + 1))
        if [ "$waited" -ge "$timeout" ]; then
            echo "Порт 5432 занят дольше ${timeout}с — принудительно освобождаю"
            sudo fuser -k 5432/tcp 2>/dev/null
            sleep 2
            break
        fi
    done
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

declare -A pkgs_wait_sampling
pkgs_wait_sampling["postgresql15st-server"]="postgresql15st-pg_wait_sampling"
pkgs_wait_sampling["postgresql16-server"]="postgresql16-pg_wait_sampling"
pkgs_wait_sampling["postgresql17-server"]="postgresql17-pg_wait_sampling"
pkgs_wait_sampling["postgresql18-server"]="postgresql18-pg_wait_sampling"

declare -A pkgs_pgaudit
pkgs_pgaudit["postgresql15st-server"]="postgresql15st-pgaudit"
pkgs_pgaudit["postgresql16-server"]="postgresql16-pgaudit"
pkgs_pgaudit["postgresql17-server"]="postgresql17-pgaudit"
pkgs_pgaudit["postgresql18-server"]="postgresql18-pgaudit"

declare -A pkgs_pgauditlogtofile
pkgs_pgauditlogtofile["postgresql15st-server"]="postgresql15st-pgauditlogtofile postgresql15st-pgaudit"
pkgs_pgauditlogtofile["postgresql16-server"]="postgresql16-pgauditlogtofile postgresql16-pgaudit"
pkgs_pgauditlogtofile["postgresql17-server"]="postgresql17-pgauditlogtofile postgresql17-pgaudit"
pkgs_pgauditlogtofile["postgresql18-server"]="postgresql18-pgauditlogtofile postgresql18-pgaudit"

for version in "${!services[@]}"; do
    svc=${services[$version]}
    data_dir="/var/lib/${svc}/data"

    extra=""
    want profile && extra="$extra ${pkgs_profile[$version]}"
    want cron && extra="$extra ${pkgs_cron[$version]}"
    want kcache && extra="$extra ${pkgs_kcache[$version]}"
    want system_stats && extra="$extra ${pkgs_system_stats[$version]}"
    want wait_sampling && extra="$extra ${pkgs_wait_sampling[$version]}"
    want pgaudit && extra="$extra ${pkgs_pgaudit[$version]}"
    want pgauditlogtofile && extra="$extra ${pkgs_pgauditlogtofile[$version]}"


    if sudo ss -ltn 2>/dev/null | grep -q ':5432 '; then
        echo "Порт 5432 уже занят перед началом итерации $version — освобождаю"
        sudo fuser -k 5432/tcp 2>/dev/null
        sleep 2
    fi

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

    if want cron || want kcache || want system_stats || want wait_sampling || want pgaudit || want pgauditlogtofile; then
        sudo systemctl stop $svc
        wait_port_free

        if [ ! -f "$data_dir/postgresql.conf" ]; then
            echo "$data_dir/postgresql.conf не создан после первого старта — что-то пошло не так, пропускаю $version"
            sudo dnf erase $version $extra -y
            continue
        fi

        declare -A seen_libs=()
        libs=""
        add_lib() {
            if [ -z "${seen_libs[$1]}" ]; then
                seen_libs[$1]=1
                libs="${libs:+$libs,}$1"
            fi
        }

        { want pgaudit || want pgauditlogtofile; } && add_lib pgaudit
        want pgauditlogtofile && add_lib pgauditlogtofile
        want kcache && { add_lib pg_stat_statements; add_lib pg_stat_kcache; }
        want wait_sampling && { add_lib pg_stat_statements; add_lib pg_wait_sampling; }
        want cron && add_lib pg_cron
        want system_stats && add_lib system_stats

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
    want wait_sampling && python3 -m pytest test_pg_wait_sampling.py -vv
    want pgaudit && python3 -m pytest test_pgaudit.py -vv
    want pgauditlogtofile && python3 -m pytest test_pgauditlogtofile.py -vv
    want restart && python3 -m pytest test_restart_resilience.py -vv

    sudo systemctl stop $svc
    wait_port_free
    sudo dnf erase $version $extra -y
done