# simSpark
a simple mini Spark project

## History

| Event                                                        | Date             |
| ------------------------------------------------------------ | ---------------- |
| Initialization                                               | 2018 Dec.18, Tue |
| Delete `driver`, add `worker` and `client` to basic structrue | 2018 Dec.18, Tue |



## Contributors

Yuhua **Wei**, Tao **Yi**

*ordered by family name*

## Structure

- simSpark
  - publib

    This directory consists of global modules of `simSpark`.

  - master

    This directory consists of the listener deployed at Master node of a `simSpark` cluster and the particular modules it needs.

  - worker

    This directory consists of the listener deployed at Workder node of a `simSpark` cluster and the particular modules it needs.

  - client

    This direcotry consists of the client application oriented to users and the particular modules it needs.