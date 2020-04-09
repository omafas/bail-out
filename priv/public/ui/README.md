# Angular Unit Tests

The unit tests are written using `Karma` (which is a a NodeJS tool for running
JavaScript in multiple browsers) and `Jasmine` (which is a popular JavaScript
testing framework.) They are commonly used together to unit test angular-built
user interfaces to the extent that they are referred to on the angular website:
https://docs.angularjs.org/guide/unit-testing.

If you've used Karma and Jasmine before, all you'll need to know is how to run
the unit tests, which is as follows:. From the command prompt in this directory,
run:

    karma start karma.conf.js

This will bring up karma in its "listening to file changes" mode and the unit
tests will run every time you save your changes in your editor. To run just one
and exit:

    karma start karma.conf.js --single-run

(Trailing options on the karma command line override options in the
configuration file.) Note that there's also a new `make` target which kick off
a sinle run of the tests. It can be run from the `ns_server` directory via:

    make ui_test

If you're not familiar  with Karma and Jasmine or if you've forgotten, you'll
need to install them. Here's how you do that. In the following I provide sample
commands for MacOS. The conversion to other package managers should hopefully be
straightforward.

## 1. Install Node.js

    sudo brew install node

(If you don't have `homebrew` then you can get that at: http://brew.sh/. I don't
believe that `sudo` ought to be required to install node, but it was for me.

## 2. Install Karma & Other Node Modules
As follows:

    sudo npm install -g karma
    sudo npm install -g karma-jasmine
    sudo npm install -g karma-jasmine-jquery
    sudo npm install -g karma-chrome-launcher

Again, `sudo` doesn't seem really necessary, but it was for me. There are
launchers to test other browsers, but I haven't tried them yet. See here for more:
http://karma-runner.github.io/0.10/config/browsers.html.
