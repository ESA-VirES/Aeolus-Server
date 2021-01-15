FROM centos:7

ARG AEOLUS_DEFINITION_FILE=AEOLUS-20200731.codadef
ARG DJANGO_VERSION=2.2.17
ARG EOXSERVER_VERSION=1.0.0rc21

# converted from scripts.d/
#   10_rpm_repos.sh
#   12_python3.sh
#
#   15_curl.sh
#   15_geotiff.sh # does not work?
#   15_jq.sh
#   15_pip.sh
#   15_unzip.sh
#   15_wget.sh
#   16_pycurl.sh
#   16_pyinotify.sh
#   20_django.sh
#   20_gdal.sh
#   20_gunicorn.sh
#   20_mapserver.sh  # TODO: some extra stuff with custom RPMs? to be handled in sub-images
#   20_msgpack.sh
#   20_netCDF4.sh
#   20_postgresql_production.sh

#   25_allauth_install.sh
#   25_requestlogging_install.sh
#   25_spacepy.sh
#   26_coda.sh
#   27_coda-aeolus.sh

# not (yet) converted:
#   15_nfs.sh               # TODO
#   20_postgresql.sh        # handled by another service
#   20_apache.sh            # handled by ingress
#   23_apache_config.sh     # same
#   31_eoxs_wsgi.sh         # handled by ingress/gunicorn
#   35_eoxs_wps_async.sh    # TODO
#   45_client_install.sh    # this is handled by another image
#   50_eoxs_instance.sh     # this needs to be performed in an entrypoint
#   55_eoxs_instance_devel_mod.sh   # -> entrypoint
#   56_client_install_static.sh # handled by
#   60_client_config.sh     # TODO
#   70_eoxs_load_fixtures.sh    # -> entrypoint


RUN yum --assumeyes install install epel-release \
    && rpm -Uvh http://yum.packages.eox.at/el/eox-release-7-0.noarch.rpm \
    && rpm -Uvh https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm \
    && yum --assumeyes install \
        python3 curl jq python-pip python3-pip unzip wget python36-pycurl pyinotify \
        gdal gdal-libs proj-epsg gdal-devel gcc-c++ python3-devel python36-numpy \
        mapserver mapserver-python3 \
        python36-msgpack \
        python36-netcdf4 \
        python3-psycopg2 postgresql11-libs \
        python-spacepy \
        coda coda-python3 \
    && pip3 install \
        "django==${DJANGO_VERSION}" \
        pygdal=="`gdal-config --version`.*" \
        gunicorn \
        django-countries \
    && pip3 install --no-deps \
        django-allauth \
        django-request-logging \
        "EOxServer==${EOXSERVER_VERSION}" \
    && wget -q -P /usr/share/coda/definitions/ https://github.com/stcorp/codadef-aeolus/releases/download/20200731/${AEOLUS_DEFINITION_FILE} \
    && curl -sSL "https://github.com/DAMATS/WPS-Backend/archive/0.5.0.tar.gz" | tar -xzf - \
    && cd "`find -name setup.py -exec dirname {} \; | head -n 1`" \
    && python3 ./setup.py install \
    && cd - \
    && yum clean all \
    && mkdir /opt/aeolus

ADD aeolus /opt/aeolus/aeolus
ADD setup.cfg setup.py README.md MANIFEST.in /opt/aeolus/

RUN pip3 install /opt/aeolus/
