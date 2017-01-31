# Base64 filter plugin for Embulk

An Embulk filter plugin to encode/decode string by Base64, Base32 and Base16.

## Overview

* **Plugin type**: filter

## Configuration

* **columns**: Input columns to encode/decode (array of hash, required)
  - **name**
    + name of input column (string, required)
  - **decode**, **encode**
    + whether to encode or decode the value (boolean, default: `false`)
    + either one must be `true` and exception is thrown when both are `true` or both are `false`
  - **encording**
    + encording type (string, default: 'Base64')
    + must be one of the follwing, Base64, Base32, Base16
  - **urlsafe**
    + whether to use urlsafe character in encoded string
    + works only when `encording: Base64`
  - **hex**
    + whether to maintain sort order of encoded string
    + works only when `encording: Base32`

## Example

### encode

See [example_encode.yml](./example/example_encode.yml) and [example_encode.csv](./example/example_encode.csv).

input:

```csv
100,A0?B1>,A0?B1>,A0?B1>,A0?B1>,A0?B1>
101,ab?cd~,ab?cd~,ab?cd~,ab?cd~,ab?cd~
```

output:

```bash
$ embulk preview example/example_encode.yml
+---------+---------------+------------------+------------------+------------------+---------------+
| id:long | Base64:string | Base64Url:string |    Base32:string | Base32Hex:string | Base16:string |
+---------+---------------+------------------+------------------+------------------+---------------+
|     100 |      QTA/QjE+ |         QTA_QjE- | IEYD6QRRHY====== | 84O3UGHH7O====== |  41303F42313E |
|     101 |      YWI/Y2R+ |         YWI_Y2R- | MFRD6Y3EPY====== | C5H3UOR4FO====== |  61623F63647E |
+---------+---------------+------------------+------------------+------------------+---------------+
```

### decode

See [example_decode.yml](./example/example_decode.yml) and [example_decode.csv](./example/example_decode.csv).

input:

```csv
100,QTA/QjE+,QTA_QjE-,IEYD6QRRHY======,84O3UGHH7O======,41303F42313E
101,YWI/Y2R+,YWI_Y2R-,MFRD6Y3EPY======,C5H3UOR4FO======,61623F63647E
```

output:

```bash
$ embulk preview example/example_decode.yml
+---------+---------------+------------------+---------------+------------------+---------------+
| id:long | Base64:string | Base64Url:string | Base32:string | Base32Hex:string | Base16:string |
+---------+---------------+------------------+---------------+------------------+---------------+
|     100 |        A0?B1> |           A0?B1> |        A0?B1> |           A0?B1> |        A0?B1> |
|     101 |        ab?cd~ |           ab?cd~ |        ab?cd~ |           ab?cd~ |        ab?cd~ |
+---------+---------------+------------------+---------------+------------------+---------------+
```

## Limitation

* Type of input value to be encoded must be string.
* Type of decoded output value will be string
  - if you want type casting, use [embulk-filter-typecast](https://github.com/sonots/embulk-filter-typecast)

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
