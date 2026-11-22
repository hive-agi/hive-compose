# orders-suite runner: POSIX sh + curl, nothing else.
FROM curlimages/curl:8.11.1
COPY --chmod=0755 run.sh /usr/local/bin/run-suite
# The runner writes its reports into a bind-mounted host dir, whose owner it
# cannot know: run as root so the write never fails on permissions.
USER root
RUN mkdir -p /reports
VOLUME ["/reports"]
ENTRYPOINT ["/usr/local/bin/run-suite"]
