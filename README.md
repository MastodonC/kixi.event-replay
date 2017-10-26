# kixi.event-replay

A tool for replaying S3 stored Event Logs into Kinesis.

## Usage

Runs as a docker based Metronome job within DCOS.

To get a change into the process commit the changes, build and new Docker image and push to Docker Hub. Jenkins jobs are configured to handle all of that occording to our usual pattern.

To execute a replay, you should use the provided script: ./deploy-replay-job. This constructs a metronome job definition, uses the dcos cli tooling to push it to the platform, before running it.

## License

Copyright © 2016 Mastodon C

Distributed under the Eclipse Public License either version 1.0 or (at your option) any later version.
