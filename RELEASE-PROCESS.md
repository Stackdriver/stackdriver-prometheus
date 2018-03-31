1.  Update file `VERSION` with the numeric version, e.g. `0.3.1`.
1.  Create a git branch for the version, e.g. `release-0.3.1`.
1.  Update `DOCKER_IMAGE_NAME` in `Makefile` to use the public docker repo. The value is provided and commented out.
1.  Run `make push`.

Updating the sample deployment configuration (`documentation/examples/prometheus-service.yml`) is still more involved:
1.  Start from the configuration for the previous release and merge any changes from the master branch.
1.  Update the version tag in the Docker image to the version you released.
1.  Apply it to your cluster to verify it works. E.g. `kubectl -f apply prometheus-service.yml`.
1.  Upload it to the public documentation repo: `gsutil cp documentation/examples/prometheus-service.yml gs://stackdriver-prometheus-documentation/`
1.  If `rbac-setup.yml` has changed since the last release, also upload it to the same repo.

This should be improved over time to include continuous integration.
