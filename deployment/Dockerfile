FROM zittix/docker-baseimage-java8
MAINTAINER Tom Coupland <tom.coupland@mastodonc.com>

CMD ["/sbin/my_init"]

ENV JAR_LOCATION=/srv/kixi.event-replay.jar
ENV CONFIG_PROFILE=${environment}

ADD target/kixi.event-replay-standalone.jar /srv/kixi.event-replay.jar
ADD scripts/run.sh /etc/service/kixi.event-replay/run

RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*