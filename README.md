# gopp

This library provides parallel processing functionality with configurable retry mechanism and error handling.

## Features

* Ability to configure the retry strategy (e.g. based on constant interval or exponential backoff) and implement additional
  strategies easily
* Ability to configure the maximum amount of retries, no matter which retry strategy is used
* Ability to route failed submissions to the caller thread for further processing that the user may want to do

## Usage

There are a couple of examples provided in the [examples](examples) folder that can help understand how to use that
library.

## Default Behavior

By default, the parallel processor will retry the submissions immediately and up to 3 times. If there is not a successful
execution, it will simply discard the failed submission.
