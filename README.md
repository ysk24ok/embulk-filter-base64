# Base64 filter plugin for Embulk

An Embulk filter plugin to encode/decode string by Base64.

## Overview

* **Plugin type**: filter

## Configuration

* columns: Input columns to encode/decode (array of hash, required)
  - **name**
    + name of input column (string, required)
  - **decode**, **encode**
    + whether to encode or decode the value (boolean, default: `false`)
    + either one must be `true` and exception is thrown if both are `true` or both are `false`

## Limitation

* Java8 environment is needed because this plugin uses [java.util.Base64](https://docs.oracle.com/javase/8/docs/api/java/util/Base64.html) of Java8
* Type of input value to be encoded must be string.
  - encoded value is string and is is needed to align the type of input and output value
    + e.g. 1234(string) is encoded into MTIzNA==(string)
* Type of output decoded value is also string
  - this plugin does nothing like type casting
    + e.g. MTIzNA==(string) is decoded into 1234(string)
  - if you want type casting, use [embulk-filter-typecast](https://github.com/sonots/embulk-filter-typecast)

## Example

### encode

See [example_encode.yml](./example/example_encode.yml) and [example_encode.csv](./example/example_encode.csv).

```bash
$ embulk preview -G example/example_encode.yml
*************************** 1 ***************************
                 id (  long) : 10
   string to encode (string) : Sm9obg==
     long to encode (string) : MTIzNDU2
   double to encode (string) : MzYuMjg=
  boolean to encode (string) : dHJ1ZQ==
timestamp to encode (string) : MjAxNi0xMi0zMSAyMzo1OTo1OQ==
     json to encode (string) : eyJhZ2UiOiAyM30=
*************************** 2 ***************************
                 id (  long) : 11
   string to encode (string) : RGF2aWQ=
     long to encode (string) : MjM0NTY3
   double to encode (string) : MTI1LjE=
  boolean to encode (string) : ZmFsc2U=
timestamp to encode (string) : MjAxNy0wMS0wMSAwMDowMDowMA==
     json to encode (string) : eyJhZ2UiOiAzNH0=
```

### decode

See [example_decode.yml](./example/example_decode.yml) and [example_decode.csv](./example/example_decode.csv).

```bash
$ embulk preview -G example/example_decode.yml
*************************** 1 ***************************
                 id (  long) : 10
   string to decode (string) : John
     long to decode (string) : 123456
   double to decode (string) : 36.28
  boolean to decode (string) : true
timestamp to decode (string) : 2016-12-31 23:59:59
     json to decode (string) : {"age": 23}
*************************** 2 ***************************
                 id (  long) : 11
   string to decode (string) : David
     long to decode (string) : 234567
   double to decode (string) : 125.01
  boolean to decode (string) : false
timestamp to decode (string) : 2017-01-01 00:00:00
     json to decode (string) : {"age": 34}
```

### decode and cast type

See [example_decode_with_typecast.yml](./example/example_decode_with_typecast.yml) and [example_decode.csv](./example/example_decode.csv).

```bash
$ embulk preview -G example/example_decode_with_typecast.yml
*************************** 1 ***************************
                 id (     long) : 10
   string to decode (   string) : John
     long to decode (     long) : 123,456
   double to decode (   double) : 36.28
  boolean to decode (  boolean) : true
timestamp to decode (timestamp) : 2016-12-31 23:59:00.590 UTC
     json to decode (     json) : {"age":23}
*************************** 2 ***************************
                 id (     long) : 11
   string to decode (   string) : David
     long to decode (     long) : 234,567
   double to decode (   double) : 125.01
  boolean to decode (  boolean) : false
timestamp to decode (timestamp) : 2017-01-01 00:00:00 UTC
     json to decode (     json) : {"age":34}
```


### Todo

* Write tests
* [Support base64 in apache commons codec](https://github.com/ysk24ok/embulk-filter-base64/issues/1)
* [Support encoder/decoder of URL and MIME](https://github.com/ysk24ok/embulk-filter-base64/issues/2)

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
