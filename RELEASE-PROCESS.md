1.  Create a git branch for the version, e.g. `0.2`, and update file `VERSION` to match.
1.  Update `DOCKER_IMAGE_NAME` in `Makefile` to use the public docker repo. The value is provided and commented out.
1.  Run `make push`.

This should be improved over time to include continuous integration.
