(if [[ `/sbin/init --version` =~ upstart ]]; then echo upstart;
elif [[ `systemctl` =~ -\.mount ]]; then echo systemd;
elif [[ -f /etc/init.d/cron && ! -h /etc/init.d/cron ]]; then echo sysv-init;
else echo none; fi) 2> /dev/null
