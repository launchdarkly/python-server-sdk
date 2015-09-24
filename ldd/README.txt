To run the tests, run:
  
  vagrant up --provision
  vagrant ssh
  cd project/ldd

Then run the desired test:

  # redis + python 2 + sync
  py2/bin/py.test test_ldd.py

  # twisted + python 2
  py2/bin/py.test --twisted test_ldd_twisted.py

  # redis + python + sync
  py3/bin/py.test test_ldd.py

If the tests don't work, you may need to restart ldd as probably went into backoff mode:

  sudo service ldd restart
