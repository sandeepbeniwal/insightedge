#!/bin/bash

DIRNAME=$(dirname ${BASH_SOURCE[0]})
source ${DIRNAME}/../conf/insightedge-env.sh

EMPTY="[]"
THIS_SCRIPT_NAME=`basename "$0"`
script="./$THIS_SCRIPT_NAME"
IE_VERSION=`grep -w "Version" ${XAP_HOME}/insightedge/VERSION | awk -F  ":" '{print $2}' | sed 's/ //'`

display_logo() {
    echo "   _____           _       _     _   ______    _            "
    echo "  |_   _|         (_)     | |   | | |  ____|  | |           "
    echo "    | |  _ __  ___ _  __ _| |__ | |_| |__   __| | __ _  ___ "
    echo "    | | | '_ \\/ __| |/ _\` | '_ \\| __|  __| / _\` |/ _\` |/ _ \\"
    echo "   _| |_| | | \\__ \\ | (_| | | | | |_| |___| (_| | (_| |  __/"
    echo "  |_____|_| |_|___/_|\\__, |_| |_|\\__|______\\__,_|\\__, |\\___|"
    echo "                      __/ |                       __/ |     "
    echo "                     |___/                       |___/   version: $IE_VERSION"
    echo "                                                            "
}

main_display_usage() {
    echo ""
    echo "Usage: ${script} [command] [args]"
    echo "Available commands:"
    display_usage_demo_inner
    display_usage_run_inner
    display_usage_deploy_space_inner
    display_usage_undeploy_inner
    display_usage_shutdown_inner
    exit 1
}

display_usage_demo_inner() {
    echo "  demo"
    echo "      Starts a demo environment on the local host"
}

display_usage_demo() {
    echo "Usage: $script demo"
    echo "      Starts a demo environment on the local host"
    echo ""
}

display_usage_run_inner() {
    echo "  run --master"
    echo "      Runs Spark Master and XAP Manager"
    echo "  run --worker [--containers=n]"
    echo "      Runs Spark Worker and n XAP Containers (default n=zero)"
    echo "  run --zeppelin"
    echo "      Runs Apache Zeppelin"
}

display_usage_run_worker() {
    echo "Usage: $script run --worker [--containers=n]"
    echo "      Runs Spark Worker and n XAP Containers (default n=zero)"
    echo ""
}

display_usage_run() {
    echo "Usage: $script run [options]"
    echo "Available options:"
    display_usage_run_inner
    echo ""
}

display_usage_deploy_space_inner() {
    echo "  deploy-space [--partitions=x [--backups]] <space-name>"
    echo "      Deploys a space with the specified name and partitions/backups (Optional)"
}

display_usage_deploy() {
    echo "Usage: $script deploy-space [--partitions=x [--backups]] <space-name>"
    echo "      Deploys a space with the specified name and partitions/backups (Optional)"
    echo ""
}

display_usage_undeploy_inner() {
    echo "  undeploy <space-name>"
    echo "      Undeploys space with the specified name"
}

display_usage_undeploy() {
    echo "Usage: $script undeploy <space-name>"
    echo "      Undeploys space with the specified name"
    echo ""
}

display_usage_shutdown_inner() {
    echo "  shutdown"
    echo "      Shuts down InsightEdge environment on the local host"
}

display_usage_shutdown() {
    echo "Usage: $script shutdown"
    echo "      Shuts down InsightEdge environment on the local host"
    echo ""
}


main() {
    display_logo
    local option=$1
    shift
    case "$option" in
      "")
        main_display_usage
        ;;
      "-h")
        main_display_usage
        ;;
      "demo")
        main_demo $@
        ;;
      "run")
        main_run $@
        ;;
      "deploy-space")
        main_deploy_space $@
        ;;
      "undeploy")
        main_undeploy $@
        ;;
      "shutdown")
        main_shutdown $@
        ;;
      *)
        error_line "Unknown option: $option"
        ;;
    esac
}


helper_stop_zeppelin() {
    step_title "--- Stopping Zeppelin"
    "${XAP_HOME}/insightedge/zeppelin/bin/zeppelin-daemon.sh" stop
}

helper_start_zeppelin() {
    step_title "--- Starting Zeppelin"
    "${XAP_HOME}/insightedge/zeppelin/bin/zeppelin-daemon.sh" start
}

helper_run_zeppelin() {
    if [ $# -ne 0 ]; then
        error_line "run zeppelin does not accept parameters"
        return
    fi
    step_title "--- Starting Zeppelin"
    ${XAP_HOME}/insightedge/zeppelin/bin/zeppelin.sh
}


step_title() {
    printf "\e[32m$1\e[0m\n"
}

error_line() {
    printf "\e[31m$1\e[0m\n"
}

handle_error() {
    error_line "$@"
    exit 1
}


# argument must be in format key=value, the function returns the value
get_option_value() {
    local arr=(${1//=/ })
    echo ${arr[1]}
}


main_demo() {
    display_demo_help() {
        printf '\e[0;34m\n'
        echo "Demo environment started:"
        echo "- Spark Master: http://${XAP_NIC_ADDRESS}:8080"
        echo "- XAP Manager: http://${XAP_NIC_ADDRESS}:8090"
        echo "- Zeppelin: http://${XAP_NIC_ADDRESS}:9090"
        printf "\e[0m\n"
    }


    if [ $# -ne 0 ]; then
        error_line "Too many arguments"
        display_usage_demo
        exit 1
    fi

    main_shutdown

    echo ""
    step_title "-- Starting gs-agent with local manager, spark master, spark worker and 2 containers..."
    #TODO print log file path
    mkdir -p "$INSIGHTEDGE_LOG_DIR"
    local log="$INSIGHTEDGE_LOG_DIR/insightedge-datagrid-local.out"

    XAP_GSA_OPTIONS="$XAP_GSA_OPTIONS -Dinsightedge.marker=master" nohup ${XAP_HOME}/bin/gs-agent.sh --manager-local --spark_master --spark_worker --gsc=2 > $log 2>&1 &

    main_deploy_space --topology=2,0 "insightedge-space"

    helper_start_zeppelin

    display_demo_help
}



main_deploy_space() {

    parse_deploy_options() {
        while [ "$1" != "" ]; do
          local option="$1"
          case ${option} in
            --topology=*)
            #TODO
              SPACE_TOPOLOGY=$(get_option_value ${option})
              if [ -z "${SPACE_TOPOLOGY}" ]; then handle_error "topology can't be empty"; fi
              ;;
            *)
              error_line "Unknown option: ${option}"
              display_usage_deploy
              exit 1
              ;;
          esac
          shift
        done
    }

    await_master_start() {
        TIMEOUT=60
        echo "  awaiting datagrid master ..."
        while [ -z "$(${XAP_HOME}/bin/gs.sh list 2>/dev/null | grep GSM)" ] ; do
            if [ $TIMEOUT -le 0 ]; then
              echo "Datagrid master is not available within timeout"
#              return
              exit 1
            fi
            TIMEOUT=$((TIMEOUT - 10))
            echo "  .. ($TIMEOUT sec)"
        done
    }


    if [ $# -eq 0 ]; then
        error_line "Space name must be specified"
        display_usage_deploy
        exit 1
    fi

    local args=( "$@" )

    #last argument is spaceName
    local SPACE_NAME="${args[${#args[@]}-1]}"
    local SPACE_TOPOLOGY="1,0"

#    echo "LAST: ${args[${#args[@]}-1]}"
    unset "args[${#args[@]}-1]"
#    echo "New without last: ${args[@]}"

    parse_deploy_options ${args[@]}

    echo ""
    step_title "-- Deploying space '$SPACE_NAME' with $SPACE_TOPOLOGY partitions..."
    await_master_start #TODO: revisit in IE-87
    ${XAP_HOME}/bin/gs.sh deploy-space -cluster schema=partitioned-sync2backup total_members=${SPACE_TOPOLOGY} ${SPACE_NAME}
}

main_undeploy() {
    local spaceName="$1"

    if [ "$spaceName" == "" ]; then
        error_line "Space name must be specified"
        display_usage_undeploy
        exit 1
    elif [ $# -ne 1 ]; then
        error_line "Too many arguments"
        display_usage_undeploy
        exit 1
    fi
    echo ""
    step_title "Undeploying space ${spaceName}"
    ${XAP_HOME}/bin/gs.sh undeploy ${spaceName}
    step_title "--- Done undeploying space"
}

main_shutdown() {
    if [ $# -ne 0 ]; then
        error_line "Too many arguments"
        display_usage_shutdown
        exit 1
    fi

    helper_stop_zeppelin
    helper_stop_agent
}

helper_run_master() {

    check_already_started_run_master() {
        pid=`ps aux | grep -v grep | grep insightedge.marker=master | awk '{print $2}'`
        if [ ! -z "$pid" ]; then
            echo "Datagrid master is already running. pid: $pid"
            exit 1
        fi
    }

    if [ $# -ne 0 ]; then
        error_line "Too many arguments"
        display_usage_run
        exit 1
    fi

    check_already_started_run_master

    step_title "Starting gs-agent with --manager --spark_master..."
    XAP_GSA_OPTIONS="$XAP_GSA_OPTIONS -Dinsightedge.marker=master" ${XAP_HOME}/bin/gs-agent.sh --manager --spark_master
}

helper_stop_master() {
    echo ""
    step_title "--- Stopping datagrid master" #TODO

    do_stop_ie_master() {
        pid=`ps aux | grep -v grep | grep insightedge.marker=master | awk '{print $2}'`
        if [ -z "$pid" ]; then
            echo "Datagrid master is not running"
            return
#            exit
        fi
        echo "Stopping datagrid master (pid: $pid)..."

        kill -SIGTERM $pid

        TIMEOUT=60
        while ps -p $pid > /dev/null; do
            if [ $TIMEOUT -le 0 ]; then
                echo "Timed out"
                return
            fi
            echo "  waiting termination ($TIMEOUT sec)"
            ((TIMEOUT--))
            sleep 1
        done
        echo "Datagrid master stopped"
    }

    do_stop_ie_master
    step_title "--- Datagrid master stopped"
}

helper_run_worker() {
    define_defaults_run_worker() {
        GSC_COUNT="0"
    }

    parse_options_run_worker() {
        while [ "$1" != "" ]; do
          local option="$1"
          case ${option} in
            --containers)
              error_line "Missing value for --containers"
              display_usage_run_worker
              exit 1
              ;;
            --containers=*)
              GSC_COUNT=$(get_option_value ${option})
              if [ -z "${GSC_COUNT}" ]; then
                  error_line "Missing value for --containers"
                  display_usage_run_worker
                  exit 1
              fi
              ;;
            *)
              error_line "Unknown option: ${option}"
              display_usage_run_worker
              exit
              ;;
          esac
          shift
        done
    }



    check_already_started_run_worker() {
        pid=`ps aux | grep -v grep | grep insightedge.marker=worker | awk '{print $2}'`
        if [ ! -z "$pid" ]; then
            echo "Worker is already running. pid: $pid"
#            return
            exit 1
        fi
    }
    define_defaults_run_worker
    parse_options_run_worker $@
    check_already_started_run_worker

    echo ""
    step_title "Starting gs-agent with --spark_worker..."
    XAP_GSA_OPTIONS="$XAP_GSA_OPTIONS -Dinsightedge.marker=worker" ${XAP_HOME}/bin/gs-agent.sh --gsc=${GSC_COUNT} --spark_worker
}

helper_stop_worker() {

    echo ""
    step_title "--- Killing gs-agent" #TODO

    do_stop_worker() {
        pid=`ps aux | grep -v grep | grep insightedge.marker=worker | awk '{print $2}'`
        if [ -z "$pid" ]; then
            echo "Datagrid worker is not running"
#            exit
            return
        fi
        echo "Stopping datagrid worker (pid: $pid)..."

        kill -SIGTERM $pid

        TIMEOUT=60
        while ps -p $pid > /dev/null; do
            if [ $TIMEOUT -le 0 ]; then
                echo "Timed out"
                return
            fi
            echo "  waiting termination ($TIMEOUT sec)"
            ((TIMEOUT--))
            sleep 1
        done
        echo "Datagrid worker stopped"
    }

    do_stop_worker
    step_title "--- Datagrid worker instances stopped"
}

helper_stop_agent() {

    echo ""
    step_title "--- Stopping gs-agent" #TODO

    pid=`ps aux | grep -v grep | grep insightedge.marker | awk '{print $2}' | tr '\n' ' '`
    if [ -z "$pid" ]; then
        echo "gs-agent is not running"
        return
    fi
    echo "PIDs found: $pid..."

    #TODO change to -9 ?
    kill -SIGTERM $pid #TODO handle multiple pids

    TIMEOUT=60
    while ps -p $pid > /dev/null; do
        if [ $TIMEOUT -le 0 ]; then
            echo "Timed out"
            return
        fi
        if ! ((TIMEOUT % 10)); then
            echo "  waiting termination ($TIMEOUT sec)"
        fi
        ((TIMEOUT--))
        sleep 1
    done


    echo "gs-agent stopped"
}

main_run() {

    local option=$1
    shift
    case "$option" in
    "")
        echo "Nothing to run"
        display_usage_run
        exit 1
        ;;
    "--master")
        helper_run_master $@
        ;;
    "--worker")
        helper_run_worker $@
        ;;
    "--zeppelin")
        helper_run_zeppelin $@
        ;;
    *)
        error_line "Unknown option: $option"
        display_usage_run
        exit 1
        ;;
    esac
}

main "$@"