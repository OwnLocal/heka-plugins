# To work on these plugins:

1. Install [goat](https://github.com/mediocregopher/goat) (just
   download the latest release and put the appropriate executable
   somewhere on your path as `goat`).
2. Install cmake, probably via `brew install cmake`.
3. Run `./devsetup.sh` to get dependencies and do build steps required
   by Heka.

Now you should be able to run `goat test` and have all the tests run.


# Running the tests

While developing, you should be running tests constantly.

1. Install a notifier plugin via `brew install terminal-notifier` or
   `sudo apt-get install notify-send`
2. Run `./runtests.sh`. Let it keep running while you edit the code,
   it will run the tests whenever you make changes.


# Making a Heka build that includes these plugins

1. Follow steps 1-2 from "To work on these plugins" above.
2. Run `./build_heka.sh`.

Now you should have a heka-xxxxxxx.tar.gz at the top level of the repo.
