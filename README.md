lipton
======

Hadoop streaming library and tool sets for python.

examples
---------
See examples in "examples" directory.

word-counter.py   word counter demo

mapper-only-example.py  mapper only hadoop job

combiner-example.py  use combiner to increase speed

How to run?
-----------

python -mlipton you-script.py -i "input" -o "output"

or

python -mlipton -h to see help.

How to debug?
-------------
If your script failed, the python exception will be logged. You can see the error message such as "see log/...." at your terminal. 


