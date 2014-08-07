#! /bin/bash
#
# chkconfig: - 99 99
# description: mysql2graphite
# processname: mysql2graphite
# pidfile: /var/run/mysql2graphite.pid
# Source function library.
. /etc/init.d/functions

RETVAL=0

# See how we were called.

prog="mysql2graphite"

start() {
        echo -n $"Starting $prog: "

        /usr/local/bin/$prog -a start
        RETVAL=$?
        echo
        [ $RETVAL -eq 0 ] && return $RETVAL
}

stop() {
        echo -n $"Stopping $prog: "
        /usr/local/bin/$prog -a stop
        RETVAL=$?
        echo
        [ $RETVAL -eq 0 ] && return $RETVAL
}

status() {
        /usr/local/bin/$prog -a status
}

restart() {
        /usr/local/bin/$prog -a restart
}

case "$1" in
  start)
        start
        ;;
  stop)
        stop
        ;;
  restart)
        restart
        ;;
  status)
        status
        ;;
  *)
        echo $"Usage: $0 {start|stop|status|restart}"
        exit 1
esac

exit $?