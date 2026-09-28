# Metabase MaxCompute Driver

> [!WARNING]
> **This repository is deprecated and no longer maintained.** The last release here,
> `1.0.0-SNAPSHOT-0.0.2`, was published on 2024-08-29 and was built against Metabase 0.50 with
> ODPS JDBC 3.6.0. It is outside the supported window of the maintained driver, is not tested
> against any newer Metabase release, and defects in it are not fixed here.
>
> The MaxCompute driver for Metabase is maintained in one place: the
> [`metabase-maxcompute-driver`](https://github.com/aliyun/aliyun-maxcompute-data-collectors/tree/master/metabase-maxcompute-driver)
> directory of
> [`aliyun/aliyun-maxcompute-data-collectors`](https://github.com/aliyun/aliyun-maxcompute-data-collectors).
> That directory is the source of truth for release assets (GitHub Releases tagged
> `metabase-<driver-version>`, with `SHA256SUMS`, SBOM and build provenance), the supported
> Metabase window and verification matrix (`compatibility.yaml`), install and build instructions,
> and issue intake.
>
> If you already have `maxcompute.metabase-driver.jar` in your `plugins` directory, replace it with
> the current driver JAR and remove any ODPS JDBC JAR installed beside it.

One defect to know about before installing anything from this repository: a query that returns an
`ARRAY` column fails with `class java.util.ArrayList cannot be cast to class java.sql.Array`,
because `ResultSet.getObject()` on such columns did not return the type the driver declared. It is
fixed in driver 0.1.1 of the maintained project, not here. Until you switch, project those
columns as text (for example `TO_JSON(col)`) instead of selecting them raw.

## Installation

Beginning with Metabase 0.32, drivers must be stored in a `plugins` directory in the same directory
where `metabase.jar` is, or you can specify the directory by setting the environment
variable `MB_PLUGINS_DIR`.

### Download Metabase Jar and Run

1. Download a fairly recent Metabase binary release (jar file) from
   the [Metabase distribution page](https://metabase.com/start/jar.html).
2. Download the MaxCompute driver jar from the maintained project's
   ["Releases"](https://github.com/aliyun/aliyun-maxcompute-data-collectors/releases) page (the asset is
   `maxcompute-metabase-driver-<driver-version>.jar`). The
   ["Releases"](https://github.com/dingxin-tech/metabase-maxcompute-driver/releases) of this repository are
   legacy builds for Metabase 0.50 and are not maintained; install the maintained artifact instead.
3. Create a directory and copy the `metabase.jar` to it.
4. In that directory create a sub-directory called `plugins`.
5. Copy the MaxCompute driver jar to the `plugins` directory.
6. Make sure you are in the directory where your `metabase.jar` lives.
7. Run `java -jar metabase.jar`.

In either case, you should see a message on startup similar to:

```
04-15 06:14:08 DEBUG plugins.lazy-loaded-driver :: Registering lazy loading driver :maxcompute...
04-15 06:14:08 INFO driver.impl :: Registered driver :maxcompute (parents: [:sql-jdbc]) 🚚
```

## Configuring

Once you've started up Metabase, go to add a database and select "MaxCompute".
You'll need to provide the [MaxCompute endpoint](https://help.aliyun.com/zh/maxcompute/user-guide/endpoints), access key, secret key, and project information.

Please note:

- The provided project must be in the same region you specify.
- The initial sync can take some time depending on how many databases and tables you have.

If you need an example policy for providing read-only access to your customer-base, make sure to
consult the Alibaba Cloud documentation.

## Contributing

### Prerequisites

- [Install Metabase core](https://github.com/metabase/metabase/wiki/Writing-a-Driver:-Packaging-a-Driver-&-Metabase-Plugin-Basics#installing-metabase-core-locally)

### Build from Source

Build the driver from the [maintained source](https://github.com/aliyun/aliyun-maxcompute-data-collectors/tree/master/metabase-maxcompute-driver)
with the reproducible build script documented there. The steps below apply only to this legacy tree
and are kept for reference.

1. Clone the repository:

```shell
git clone https://github.com/dingxin-tech/metabase-maxcompute-driver
cd repository
```
2. replace the metabase source code directory in `deps.edn`
3. Build the project

```shell
clojure -X:build
```

You should now have a `maxcompute.metabase-driver.jar` file in the `target` directory.

4. Download a fairly recent Metabase binary release (jar file) from
   the [Metabase distribution page](https://metabase.com/start/jar.html).

5. Let's assume we download `metabase.jar` to `~/metabase/` and we built the project above. Copy the
   built jar to the Metabase plugins directly and run Metabase from there!

```shell
TARGET_DIR=~/metabase
mkdir ${TARGET_DIR}/plugins/
cp target/maxcompute.metabase-driver.jar ${TARGET_DIR}/plugins/
cd ${TARGET_DIR}/
java -jar metabase.jar
```

You should see a message on startup similar to:

```
2019-05-07 23:27:32 INFO plugins.lazy-loaded-driver :: Registering lazy loading driver :maxcompute...
2019-05-07 23:27:32 INFO metabase.driver :: Registered driver :maxcompute (parents: #{:sql-jdbc}) 🚚
```

### Testing

Testing is not yet implemented for this driver. However, we encourage users to report any issues
encountered during usage. Testing will be added in future updates.

### Contributing
This driver is developed based on Metabase version 0.50. If you find it incompatible with a specific version, please don't hesitate to contact us. 
Any form of contribution is also welcome.

## Issues

If you encounter any issues or have questions, please open an issue in the
[maintained project](https://github.com/aliyun/aliyun-maxcompute-data-collectors/issues); Metabase driver reports are
triaged against the `metabase-maxcompute-driver` component. Issues opened in this repository are not triaged.

Thank you for using the Metabase MaxCompute Driver!