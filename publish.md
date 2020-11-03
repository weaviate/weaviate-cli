1. Make sure you set the right version in the `setup.py`
2. Then build the new package:\
`python setup.py bdist_wheel`
3. And check it:\
`twine check dist/*`
4. Check if you are on **Master** in case of full release.
5. **Commit** the most current version to GitHub if this has not been done yet.
6. Make sure you gave the commit of the current version a proper tag:\
`git tag -a '<your tag>' -m '<some message for the tag>' && git push --tags`
tags are either in the form of `0.2.5` or `0.2.5rc0`.
7. Finally publish:\
`twine upload dist/*`
8. After publishing change the version in `setup.py` to the next developement number.