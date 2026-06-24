# File-based catalog (FBC) image for rbln-npu-operator.
# Built from the rendered $(CATALOG_DIR)/ produced by `make catalog`.
# Serves the catalog over gRPC for a CatalogSource (sourceType: grpc).
FROM quay.io/operator-framework/opm:v1.46.0

ENTRYPOINT ["/bin/opm"]
CMD ["serve", "/configs", "--cache-dir=/tmp/cache"]

ADD catalog /configs
LABEL operators.operatorframework.io.index.configs.v1=/configs

# Pre-build the serve cache so the CatalogSource pod starts fast.
RUN ["/bin/opm", "serve", "/configs", "--cache-dir=/tmp/cache", "--cache-only"]
