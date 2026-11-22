# hog as an image, for a container suite runner (S13b): busybox and the
# script, nothing else. The compose.yml next to it bind-mounts the script
# instead, so the by-hand path needs no build.
FROM busybox:1.36
COPY --chmod=0755 hog.sh /usr/local/bin/hog
ENTRYPOINT ["/usr/local/bin/hog"]
