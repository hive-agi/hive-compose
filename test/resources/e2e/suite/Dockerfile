# orders-suite runner: POSIX sh + curl, nothing else.
FROM curlimages/curl:8.11.1
COPY --chmod=0755 run.sh /usr/local/bin/run-suite
# The runner writes its reports here; mount a host dir over it.
USER root
RUN mkdir -p /reports && chown curl_user:curl_group /reports
USER curl_user
VOLUME ["/reports"]
ENTRYPOINT ["/usr/local/bin/run-suite"]
