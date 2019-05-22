python setup.py bdist_wheel
python -m twine upload dist/* -u semi-technologies
rm -r dist/
