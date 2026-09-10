# M6.5. The console's own Dockerfile, and it is deliberately NOT the repository's root Dockerfile.
#
# That one takes a MODULE build-arg and copies each Maven module's POM and src by name. `ui/` is
# not a Maven module, must not be in the parent POM, and must not be added to those COPY lists -
# the project rule "add every new module to the Dockerfile and the parent POM" is about the Maven
# reactor, and applying it here breaks the build for every service.
#
# Two stages, and the node stage is discarded. Shipping it would mean node, npm and ~200 MB of
# node_modules in a container whose entire job is to serve static files - on a machine where RAM
# and disk are the binding constraint. What reaches the final image is the contents of dist/.

# ---------------------------------------------------------------------------------------------
FROM node:24-alpine AS build

WORKDIR /app

# package.json and the lockfile FIRST, on their own layer. Docker caches a layer by the files it
# copies, so installing before the sources are copied means a one-character change to a component
# does not re-run `npm ci`. Copying everything at once is the commonest reason a frontend image
# takes three minutes to rebuild.
COPY package.json package-lock.json ./

# `npm ci`, never `npm install`. ci installs exactly what the lockfile pins and fails if
# package.json and the lockfile disagree; install is allowed to resolve something newer and
# rewrite the lockfile, which makes the image non-reproducible in the quietest possible way - the
# build passes, with different dependency versions from the one that was tested.
RUN npm ci

COPY tsconfig.json vite.config.ts index.html ./
COPY src ./src

# `npm run build` runs `tsc -b` before vite. That is on purpose: vite alone strips types without
# checking them, so a type error would ship. Here it fails the image build instead.
RUN npm run build

# ---------------------------------------------------------------------------------------------
FROM nginx:1.29-alpine AS runtime

# The default site has to be replaced rather than added to - nginx's packaged default.conf also
# listens on 80 and would win or clash depending on load order.
RUN rm /etc/nginx/conf.d/default.conf
COPY nginx.conf /etc/nginx/conf.d/dpe.conf
COPY dpe-proxy.conf /etc/nginx/dpe-proxy.conf

COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80

# Requesting index.html rather than testing the config, because `nginx -t` would report healthy
# while the static root was empty and every backend unreachable.
#
# 127.0.0.1, NOT localhost, and this cost a debugging round: nginx here listens on 0.0.0.0:80
# (IPv4 only - the config has one `listen 80;` and no `listen [::]:80;`), while `localhost` inside
# the container resolves to ::1 first. busybox wget tries IPv6, gets ECONNREFUSED, and never falls
# back to IPv4. The container is perfectly healthy and reports "wget: can't connect to remote
# host", which reads like nginx failed to start.
HEALTHCHECK --interval=10s --timeout=5s --retries=5 --start-period=5s \
  CMD wget -qO- http://127.0.0.1/index.html > /dev/null || exit 1
