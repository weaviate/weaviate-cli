1. Set the new version in the `weaviate/version.py` and `test/test_version.py`.
2. Run all tests. (Check `test/README.md`)
3. Check if you are on the correct **GitBranch**.
4. **Commit** the most current version to GitHub if this has not been done yet.
5. Give the commit of the current version a proper tag:\
`git tag -a '<your tag>' -m '<some message for the tag>' && git push --tags`
Tags are either in the form of `v0.2.5` or `v0.2.5rc0`.
6. The release workflow defined in `.github/workflows/release.yaml` will take care of building, creating the release, and pushing to PyPI.
