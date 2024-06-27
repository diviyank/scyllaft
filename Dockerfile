FROM ghcr.io/pyo3/maturin
WORKDIR /
RUN yum update -y && yum install -y perl-core openssl openssl-devel pkgconfig libatomic
WORKDIR /io
ENTRYPOINT ["/usr/bin/maturin"]
