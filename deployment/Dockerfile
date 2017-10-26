FROM zittix/docker-baseimage-java8
MAINTAINER Tom Coupland <tom.coupland@mastodonc.com>

ENV JAR_LOCATION=/srv/kixi.event-replay.jar
ENV CONFIG_PROFILE=${environment}

ADD target/kixi.event-replay-standalone.jar /srv/kixi.event-replay.jar
ADD scripts/run.sh /srv/run

CMD ["/sbin/my_init", "--", "/srv/run"]

RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*