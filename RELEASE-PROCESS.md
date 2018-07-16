# Release Process

You are able to automate section 1 and 2 below by running `./release.sh {VERSION}`. Section 3 is not automated due to the manual verification step. This should be improved over time to include continuous integration.

## 1. Build and release docker image

1.  Update file `VERSION` with the numeric version, e.g. `0.3.1`.
1.  Create a git branch for the version, e.g. `release-0.3.1`.
1.  Run `DOCKER_IMAGE_NAME={public_docker_image} make push`.

## 2. Deploy Prometheus server

Updating the sample deployment configuration (`documentation/examples/prometheus-service.yml`) is still more involved:

1.  Start from the configuration for the previous release and merge any changes from the master branch.
1.  Update the container image in `documentation/examples/prometheus-service.yml` to the image you released.
1.  Apply it to your cluster to verify it works. E.g. `kubectl apply -f documentation/examples/prometheus-service.yml`.

## 3. Publish configuration files

These steps should not be performed until you are confident that the configuration files and the associated docker images are stable, as this will override the configuration files that are included in public documentation.

1.  Run the e2e tests located at https://github.com/Stackdriver/stackdriver-prometheus-e2e, if they do not pass, please debug and resolve any issues before proceeding.
1.  Upload it to the public documentation repo: `gsutil cp documentation/examples/prometheus-service.yml gs://stackdriver-prometheus-documentation/`
1.  If `rbac-setup.yml` has changed since the last release, also upload it to the same repo.
