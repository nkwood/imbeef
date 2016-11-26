# imbeef

imbeef acts as a consumer to the [Amazon Kinesis](https://aws.amazon.com/kinesis) stream written to by [p25dcodr](https://github.com/radiowitness/p25dcodr) hosts. [IMBE audio chunks](https://en.wikipedia.org/wiki/Multi-Band_Excitation) are extracted from the stream of P25 frames and transcoded to WAVE files, which are then stored in [Amazon S3](https://aws.amazon.com/s3/).

## Create imbeef.properties
Copy `example-imbeef.properties` to `imbeef.properties` and modify as you see fit.

## Build
```
$ mvn package
```

## Run
```
$ java -jar target/imbeef-x.x.x.jar
```

## License

Copyright 2016 An Honest Effort LLC

Licensed under the GPLv3: http://www.gnu.org/licenses/gpl-3.0.html
