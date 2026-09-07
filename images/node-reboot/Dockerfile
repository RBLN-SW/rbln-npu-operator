FROM alpine:3.22

# apk upgrade pulls package-level security fixes (e.g. openssl) that land in
# the Alpine repos before the base image tag is rebuilt with them.
RUN apk upgrade --no-cache && apk add --no-cache util-linux sudo

ENTRYPOINT ["/bin/sh"]
